from __future__ import annotations

import math
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional

from .config import Settings
from .db import Database
from .schemas import EventIn


def iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def to_epoch(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class TelemetryService:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings

    # ------------------------------------------------------------------ ingest

    def ingest(self, device_id: str, events: list[EventIn]) -> dict:
        """整批在一个事务内处理：去重 -> 入窗/修正 -> 推进水位线 -> 封存窗口。"""
        now = time.time()
        wsize = self.settings.window_size_seconds
        lateness = self.settings.allowed_lateness_seconds

        results = []
        accepted = late = duplicates = corrections = 0

        with self.db.transaction() as conn:
            device = self._get_or_create_device(conn, device_id, now)
            max_event_time = device["max_event_time"]
            # 批前水位线：用于判断“该窗口是否早已封存”（库里还没有该窗口行的情况）
            watermark = (
                max_event_time - lateness if max_event_time is not None else None
            )

            for ev in events:
                ts = to_epoch(ev.event_time)
                ws = math.floor(ts / wsize) * wsize
                we = ws + wsize

                if self._event_exists(conn, ev.event_id):
                    duplicates += 1
                    results.append(
                        {"event_id": ev.event_id, "status": "duplicate",
                         "window_start": iso(ws), "window_status": None}
                    )
                    continue

                window = conn.execute(
                    "SELECT * FROM windows WHERE device_id=? AND window_start=?",
                    (device_id, ws),
                ).fetchone()

                sealed = window is not None and window["status"] == "sealed"
                if window is None and watermark is not None and we <= watermark:
                    # 窗口从未出现，但水位线已越过 -> 直接以封存态创建
                    self._create_window(conn, device_id, ws, we, ev, now,
                                        status="sealed", corrections=1)
                    sealed = True
                elif window is None:
                    self._create_window(conn, device_id, ws, we, ev, now,
                                        status="open", corrections=0)
                elif sealed:
                    self._apply_to_window(conn, device_id, ws, ev, correction=True)
                else:
                    self._apply_to_window(conn, device_id, ws, ev, correction=False)

                if sealed:
                    late += 1
                    corrections += 1
                    status = "late"
                    self._record_correction(conn, device_id, ws, ev, now)
                else:
                    accepted += 1
                    status = "accepted"
                    # 只有正常接收的事件才推进事件时间水位线
                    if max_event_time is None or ts > max_event_time:
                        max_event_time = ts

                self._insert_event(conn, device_id, ev, ts, status, now)
                results.append(
                    {"event_id": ev.event_id, "status": status,
                     "window_start": iso(ws),
                     "window_status": "sealed" if sealed else "open"}
                )

            watermark = (
                max_event_time - lateness if max_event_time is not None else None
            )
            conn.execute(
                "UPDATE devices SET max_event_time=?, watermark=? WHERE device_id=?",
                (max_event_time, watermark, device_id),
            )
            if watermark is not None:
                conn.execute(
                    "UPDATE windows SET status='sealed', sealed_at=? "
                    "WHERE device_id=? AND status='open' AND window_end <= ?",
                    (now, device_id, watermark),
                )

        return {
            "device_id": device_id,
            "accepted": accepted,
            "late": late,
            "duplicates": duplicates,
            "corrections": corrections,
            "watermark": iso(watermark),
            "results": results,
        }

    # ------------------------------------------------------------------ queries

    def get_device(self, device_id: str) -> Optional[dict]:
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM devices WHERE device_id=?", (device_id,)
            ).fetchone()
        return self._device_out(row) if row else None

    def list_windows(self, device_id: str,
                     status: Optional[str] = None) -> Optional[dict]:
        with self.db.transaction() as conn:
            device = conn.execute(
                "SELECT * FROM devices WHERE device_id=?", (device_id,)
            ).fetchone()
            if device is None:
                return None
            sql = ("SELECT * FROM windows WHERE device_id=?")
            params: list = [device_id]
            if status:
                sql += " AND status=?"
                params.append(status)
            sql += " ORDER BY window_start"
            rows = conn.execute(sql, params).fetchall()
        return {
            "device": self._device_out(device),
            "windows": [self._window_out(r) for r in rows],
        }

    def list_corrections(self, device_id: str) -> list[dict]:
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM corrections WHERE device_id=? ORDER BY id",
                (device_id,),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "device_id": r["device_id"],
                "window_start": iso(r["window_start"]),
                "event_id": r["event_id"],
                "event_time": iso(r["event_time"]),
                "value": r["value"],
                "created_at": iso(r["created_at"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------ internals

    @staticmethod
    def _get_or_create_device(conn: sqlite3.Connection,
                              device_id: str, now: float) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO devices (device_id, max_event_time, watermark, created_at)"
                " VALUES (?, NULL, NULL, ?)",
                (device_id, now),
            )
            row = conn.execute(
                "SELECT * FROM devices WHERE device_id=?", (device_id,)
            ).fetchone()
        return row

    @staticmethod
    def _event_exists(conn: sqlite3.Connection, event_id: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM events WHERE event_id=?", (event_id,)
        ).fetchone() is not None

    @staticmethod
    def _insert_event(conn: sqlite3.Connection, device_id: str,
                      ev: EventIn, ts: float, status: str, now: float) -> None:
        conn.execute(
            "INSERT INTO events (event_id, device_id, event_time, value, status, ingested_at)"
            " VALUES (?,?,?,?,?,?)",
            (ev.event_id, device_id, ts, ev.value, status, now),
        )

    @staticmethod
    def _create_window(conn: sqlite3.Connection, device_id: str,
                       ws: float, we: float, ev: EventIn, now: float,
                       status: str, corrections: int) -> None:
        conn.execute(
            "INSERT INTO windows (device_id, window_start, window_end, status,"
            " event_count, sum_value, min_value, max_value, corrections, sealed_at)"
            " VALUES (?,?,?,?,1,?,?,?,?,?)",
            (device_id, ws, we, status, ev.value, ev.value, ev.value,
             corrections, now if status == "sealed" else None),
        )

    @staticmethod
    def _apply_to_window(conn: sqlite3.Connection, device_id: str,
                         ws: float, ev: EventIn, correction: bool) -> None:
        conn.execute(
            "UPDATE windows SET"
            " event_count = event_count + 1,"
            " sum_value   = sum_value + ?,"
            " min_value   = MIN(min_value, ?),"
            " max_value   = MAX(max_value, ?),"
            " corrections = corrections + ?"
            " WHERE device_id=? AND window_start=?",
            (ev.value, ev.value, ev.value, 1 if correction else 0, device_id, ws),
        )

    @staticmethod
    def _record_correction(conn: sqlite3.Connection, device_id: str,
                           ws: float, ev: EventIn, now: float) -> None:
        conn.execute(
            "INSERT INTO corrections (device_id, window_start, event_id,"
            " event_time, value, created_at) VALUES (?,?,?,?,?,?)",
            (device_id, ws, ev.event_id, to_epoch(ev.event_time), ev.value, now),
        )

    @staticmethod
    def _device_out(row: sqlite3.Row) -> dict:
        return {
            "device_id": row["device_id"],
            "max_event_time": iso(row["max_event_time"]),
            "watermark": iso(row["watermark"]),
            "created_at": iso(row["created_at"]),
        }

    @staticmethod
    def _window_out(r: sqlite3.Row) -> dict:
        count = r["event_count"]
        return {
            "window_start": iso(r["window_start"]),
            "window_end": iso(r["window_end"]),
            "status": r["status"],
            "event_count": count,
            "sum": r["sum_value"],
            "avg": (r["sum_value"] / count) if count else None,
            "min": r["min_value"],
            "max": r["max_value"],
            "corrections": r["corrections"],
            "sealed_at": iso(r["sealed_at"]),
        }
