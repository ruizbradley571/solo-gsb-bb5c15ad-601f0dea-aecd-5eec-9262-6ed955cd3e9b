from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id       TEXT PRIMARY KEY,
    max_event_time  REAL,               -- 已见最大事件时间（epoch 秒）
    watermark       REAL,               -- = max_event_time - allowed_lateness
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,   -- 幂等去重键
    device_id       TEXT NOT NULL,
    event_time      REAL NOT NULL,
    value           REAL NOT NULL,
    status          TEXT NOT NULL,      -- 'accepted' | 'late'
    ingested_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_device ON events(device_id, event_time);

CREATE TABLE IF NOT EXISTS windows (
    device_id       TEXT NOT NULL,
    window_start    REAL NOT NULL,
    window_end      REAL NOT NULL,
    status          TEXT NOT NULL,      -- 'open' | 'sealed'
    event_count     INTEGER NOT NULL DEFAULT 0,
    sum_value       REAL NOT NULL DEFAULT 0,
    min_value       REAL,
    max_value       REAL,
    corrections     INTEGER NOT NULL DEFAULT 0,
    sealed_at       REAL,
    PRIMARY KEY (device_id, window_start)
);

CREATE TABLE IF NOT EXISTS corrections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    window_start    REAL NOT NULL,
    event_id        TEXT NOT NULL,
    event_time      REAL NOT NULL,
    value           REAL NOT NULL,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corrections_device
    ON corrections(device_id, window_start);
"""


class Database:
    """单连接 + 写锁的轻量 SQLite 封装，足以支撑本服务规模。"""

    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()

    def init(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        self._conn.close()
