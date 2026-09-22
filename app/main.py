from __future__ import annotations

from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request

from .config import Settings
from .db import Database
from .schemas import (
    CorrectionOut,
    DeviceOut,
    IngestBatch,
    IngestResponse,
    WindowsResponse,
)
from .service import TelemetryService


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    db = Database(settings.db_path)
    db.init()
    service = TelemetryService(db, settings)

    app = FastAPI(
        title="Sensor Telemetry Aggregation API",
        version="1.0.0",
        description=(
            "按设备维护事件时间水位线与允许迟到窗口；"
            "对 event_id 幂等去重；窗口封存后到达的数据标记为 late 并生成修正记录。"
        ),
    )
    app.state.service = service
    app.state.db = db

    def get_service(request: Request) -> TelemetryService:
        return request.app.state.service

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/v1/ingest", response_model=IngestResponse)
    def ingest(batch: IngestBatch,
               svc: TelemetryService = Depends(get_service)) -> dict:
        return svc.ingest(batch.device_id, batch.events)

    @app.get("/v1/devices/{device_id}", response_model=DeviceOut)
    def get_device(device_id: str,
                   svc: TelemetryService = Depends(get_service)) -> dict:
        device = svc.get_device(device_id)
        if device is None:
            raise HTTPException(status_code=404, detail="device not found")
        return device

    @app.get("/v1/devices/{device_id}/windows", response_model=WindowsResponse)
    def list_windows(
        device_id: str,
        status: Optional[str] = Query(default=None, pattern="^(open|sealed)$"),
        svc: TelemetryService = Depends(get_service),
    ) -> dict:
        result = svc.list_windows(device_id, status)
        if result is None:
            raise HTTPException(status_code=404, detail="device not found")
        return result

    @app.get("/v1/devices/{device_id}/corrections",
             response_model=list[CorrectionOut])
    def list_corrections(device_id: str,
                         svc: TelemetryService = Depends(get_service)) -> list[dict]:
        return svc.list_corrections(device_id)

    return app


app = create_app()
