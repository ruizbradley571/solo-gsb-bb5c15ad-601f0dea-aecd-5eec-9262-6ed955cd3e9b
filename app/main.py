"""传感器遥测汇聚 API。

核心语义：
- 每个设备维护一条持久化的事件时间水位线 watermark = 已见最大 event_time。
- 时间窗为按 window_size_seconds 对齐 epoch 的滚动窗口。
- 当 watermark >= window_end + allowed_lateness_seconds 时，窗口封存（sealed）。
- 重复 (device_id, event_id) 幂等去重，不影响聚合结果。
- 窗口封存后到达的事件标记为 late，仍计入聚合，同时生成一条修正记录，
  窗口的 correction_count 随之递增。
"""

from __future__ import annotations

import math
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import db

DEFAULT_WINDOW_SIZE_SECONDS = 60
DEFAULT_ALLOWED_LATENESS_SECONDS = 30


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="Sensor Telemetry Aggregation API", version="1.0.0", lifespan=lifespan)


# ---------- Pydantic 模型 ----------

class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    event_time: datetime  # ISO 8601；不带时区时按 UTC 处理
    value: float


class IngestRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    events: list[EventIn] = Field(min_length=1, max_length=10000)


class EventResult(BaseModel):
    event_id: str
    status: Literal["accepted", "duplicate", "late"]
    window_start: Optional[datetime] = None


class IngestResponse(BaseModel):
    device_id: str
    watermark: Optional[datetime]
    accepted: int
    duplicates: int
    late: int
    results: list[EventResult]


class DeviceConfig(BaseModel):
    window_size_seconds: int = Field(default=DEFAULT_WINDOW_SIZE_SECONDS, gt=0, le=86400)
    allowed_lateness_seconds: int = Field(default=DEFAULT_ALLOWED_LATENESS_SECONDS, ge=0, le=86400)


class DeviceOut(BaseModel):
    device_id: str
    window_size_seconds: int
    allowed_lateness_seconds: int
    watermark: Optional[datetime]
    event_count: int
    open_windows: int
    sealed_windows: int
    created_at: datetime


class WindowOut(BaseModel):
    device_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["open", "sealed"]
    count: int
    sum: float
    avg: Optional[float]
    min: Optional[float]
    max: Optional[float]
    correction_count: int
    late_event_count: int
    sealed_at: Optional[datetime]


class CorrectionOut(BaseModel):
    id: int
    device_id: str
    event_id: str
    window_start: datetime
    event_time: datetime
    value: float
    created_at: datetime


# ---------- 工具函数 ----------

def to_epoch(dt: datetime) -> float:
    """datetime -> epoch 秒；naive 时间按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def to_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def window_floor(t: float, size: int) -> float:
    return math.floor(t / size) * size


def get_device(conn: sqlite3.Connection, device_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()


def get_or_create_device(conn: sqlite3.Connection, device_id: str, now: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        conn.execute(
            "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
            " VALUES (?, ?, ?, NULL, ?)",
            (device_id, DEFAULT_WINDOW_SIZE_SECONDS, DEFAULT_ALLOWED_LATENESS_SECONDS, now),
        )
        row = get_device(conn, device_id)
    return row


def require_device(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")
    return row


def device_out(conn: sqlite3.Connection, device_id: str) -> DeviceOut:
    dev = require_device(conn, device_id)
    stats = conn.execute(
        "SELECT"
        " (SELECT COUNT(*) FROM events WHERE device_id = ?) AS event_count,"
        " (SELECT COUNT(*) FROM windows WHERE device_id = ? AND status = 'open') AS open_windows,"
        " (SELECT COUNT(*) FROM windows WHERE device_id = ? AND status = 'sealed') AS sealed_windows",
        (device_id, device_id, device_id),
    ).fetchone()
    return DeviceOut(
        device_id=dev["device_id"],
        window_size_seconds=dev["window_size_seconds"],
        allowed_lateness_seconds=dev["allowed_lateness_seconds"],
        watermark=to_iso(dev["watermark"]),
        event_count=stats["event_count"],
        open_windows=stats["open_windows"],
        sealed_windows=stats["sealed_windows"],
        created_at=dev["created_at"],
    )


# ---------- 接口 ----------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
def ingest(req: IngestRequest) -> IngestResponse:
    """按设备提交事件批次。批次内事件按提交顺序处理（与流式语义一致）。"""
    now = utcnow_iso()
    results: list[EventResult] = []

    with db.session() as conn:
        dev = get_or_create_device(conn, req.device_id, now)
        watermark = dev["watermark"]
        size = dev["window_size_seconds"]
        lateness = dev["allowed_lateness_seconds"]

        for ev in req.events:
            t = to_epoch(ev.event_time)

            # 幂等去重：同一 (device_id, event_id) 只生效一次
            dup = conn.execute(
                "SELECT 1 FROM events WHERE device_id = ? AND event_id = ?",
                (req.device_id, ev.event_id),
            ).fetchone()
            if dup:
                results.append(EventResult(event_id=ev.event_id, status="duplicate"))
                continue

            w_start = window_floor(t, size)
            w_end = w_start + size
            # 所属窗口已封存（水位线已越过 window_end + 允许迟到窗口）=> 迟到数据
            is_late = watermark is not None and watermark >= w_end + lateness
            status = "late" if is_late else "accepted"

            conn.execute(
                "INSERT INTO events (device_id, event_id, event_time, value, status, ingested_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (req.device_id, ev.event_id, t, ev.value, status, now),
            )
            conn.execute(
                """
                INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,
                                     status, correction_count, late_event_count)
                VALUES (?, ?, ?, 1, ?, ?, ?, 'open', ?, ?)
                ON CONFLICT (device_id, window_start) DO UPDATE SET
                    count             = count + 1,
                    sum               = sum + excluded.sum,
                    min               = MIN(windows.min, excluded.min),
                    max               = MAX(windows.max, excluded.max),
                    correction_count  = correction_count + excluded.correction_count,
                    late_event_count  = late_event_count + excluded.late_event_count
                """,
                (
                    req.device_id, w_start, w_end, ev.value, ev.value, ev.value,
                    1 if is_late else 0,
                    1 if is_late else 0,
                ),
            )
            if is_late:
                # 迟到数据落入已封存窗口：生成修正记录
                conn.execute(
                    "INSERT INTO corrections (device_id, event_id, window_start, event_time, value, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (req.device_id, ev.event_id, w_start, t, ev.value, now),
                )

            if watermark is None or t > watermark:
                watermark = t
            results.append(EventResult(event_id=ev.event_id, status=status, window_start=to_iso(w_start)))

        # 持久化水位线，并封存所有越过允许迟到窗口的窗口
        conn.execute(
            "UPDATE devices SET watermark = ? WHERE device_id = ?",
            (watermark, req.device_id),
        )
        conn.execute(
            "UPDATE windows SET status = 'sealed', sealed_at = ?"
            " WHERE device_id = ? AND status = 'open' AND window_end + ? <= ?",
            (now, req.device_id, lateness, watermark),
        )

    return IngestResponse(
        device_id=req.device_id,
        watermark=to_iso(watermark),
        accepted=sum(1 for r in results if r.status == "accepted"),
        duplicates=sum(1 for r in results if r.status == "duplicate"),
        late=sum(1 for r in results if r.status == "late"),
        results=results,
    )


@app.put("/devices/{device_id}/config", response_model=DeviceOut)
def put_device_config(device_id: str, cfg: DeviceConfig) -> DeviceOut:
    """创建或更新设备配置。已有数据后不允许修改窗口大小（避免历史窗口口径不一致）。"""
    with db.session() as conn:
        dev = get_device(conn, device_id)
        if dev is None:
            conn.execute(
                "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
                " VALUES (?, ?, ?, NULL, ?)",
                (device_id, cfg.window_size_seconds, cfg.allowed_lateness_seconds, utcnow_iso()),
            )
        else:
            if cfg.window_size_seconds != dev["window_size_seconds"]:
                n = conn.execute(
                    "SELECT COUNT(*) AS c FROM windows WHERE device_id = ?", (device_id,)
                ).fetchone()["c"]
                if n > 0:
                    raise HTTPException(
                        status_code=409,
                        detail="window_size_seconds cannot be changed after data has been ingested",
                    )
            conn.execute(
                "UPDATE devices SET window_size_seconds = ?, allowed_lateness_seconds = ? WHERE device_id = ?",
                (cfg.window_size_seconds, cfg.allowed_lateness_seconds, device_id),
            )
        return device_out(conn, device_id)


@app.get("/devices/{device_id}", response_model=DeviceOut)
def get_device_info(device_id: str) -> DeviceOut:
    with db.session() as conn:
        return device_out(conn, device_id)


@app.get("/devices/{device_id}/windows", response_model=list[WindowOut])
def list_windows(
    device_id: str,
    status: Optional[Literal["open", "sealed"]] = None,
) -> list[WindowOut]:
    """返回设备各时间窗的聚合值、状态与修正次数。"""
    with db.session() as conn:
        require_device(conn, device_id)
        sql = "SELECT * FROM windows WHERE device_id = ?"
        params: list = [device_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [
            WindowOut(
                device_id=r["device_id"],
                window_start=to_iso(r["window_start"]),
                window_end=to_iso(r["window_end"]),
                status=r["status"],
                count=r["count"],
                sum=r["sum"],
                avg=r["sum"] / r["count"] if r["count"] else None,
                min=r["min"],
                max=r["max"],
                correction_count=r["correction_count"],
                late_event_count=r["late_event_count"],
                sealed_at=r["sealed_at"],
            )
            for r in rows
        ]


@app.get("/devices/{device_id}/corrections", response_model=list[CorrectionOut])
def list_corrections(device_id: str) -> list[CorrectionOut]:
    """返回设备全部修正记录（窗口封存后到达的迟到数据）。"""
    with db.session() as conn:
        require_device(conn, device_id)
        rows = conn.execute(
            "SELECT * FROM corrections WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        return [
            CorrectionOut(
                id=r["id"],
                device_id=r["device_id"],
                event_id=r["event_id"],
                window_start=to_iso(r["window_start"]),
                event_time=to_iso(r["event_time"]),
                value=r["value"],
                created_at=r["created_at"],
            )
            for r in rows
        ]
