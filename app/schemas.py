from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    event_time: datetime  # ISO 8601；缺省时区按 UTC 处理
    value: float


class IngestBatch(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    events: list[EventIn] = Field(min_length=1, max_length=10_000)


class EventResult(BaseModel):
    event_id: str
    status: Literal["accepted", "late", "duplicate"]
    window_start: Optional[str] = None
    window_status: Optional[Literal["open", "sealed"]] = None


class IngestResponse(BaseModel):
    device_id: str
    accepted: int
    late: int
    duplicates: int
    corrections: int
    watermark: Optional[str]
    results: list[EventResult]


class WindowOut(BaseModel):
    window_start: str
    window_end: str
    status: str
    event_count: int
    sum: float
    avg: Optional[float]
    min: Optional[float]
    max: Optional[float]
    corrections: int
    sealed_at: Optional[str]


class DeviceOut(BaseModel):
    device_id: str
    max_event_time: Optional[str]
    watermark: Optional[str]
    created_at: str


class WindowsResponse(BaseModel):
    device: DeviceOut
    windows: list[WindowOut]


class CorrectionOut(BaseModel):
    id: int
    device_id: str
    window_start: str
    event_id: str
    event_time: str
    value: float
    created_at: str
