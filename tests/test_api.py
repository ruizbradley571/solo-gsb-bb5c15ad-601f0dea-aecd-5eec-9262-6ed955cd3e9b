import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size_seconds=60,
        allowed_lateness_seconds=30,
    )
    return TestClient(create_app(settings))


def ingest(client, device_id, events):
    resp = client.post("/v1/ingest", json={"device_id": device_id, "events": events})
    assert resp.status_code == 200, resp.text
    return resp.json()


def ev(event_id, event_time, value):
    return {"event_id": event_id, "event_time": event_time, "value": value}


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_basic_ingest_and_window_aggregate(client):
    out = ingest(client, "dev-1", [
        ev("e1", "2026-09-22T10:00:05Z", 1.5),
        ev("e2", "2026-09-22T10:00:35Z", 2.5),
        ev("e3", "2026-09-22T10:01:10Z", 4.0),
    ])
    assert out["accepted"] == 3 and out["late"] == 0 and out["duplicates"] == 0
    assert out["watermark"] == "2026-09-22T10:00:40Z"  # 10:01:10 - 30s

    windows = client.get("/v1/devices/dev-1/windows").json()["windows"]
    assert len(windows) == 2
    w0, w1 = windows
    assert w0["window_start"] == "2026-09-22T10:00:00Z"
    assert w0["status"] == "open"
    assert (w0["event_count"], w0["sum"], w0["avg"]) == (2, 4.0, 2.0)
    assert (w0["min"], w0["max"], w0["corrections"]) == (1.5, 2.5, 0)
    assert w1["status"] == "open"  # 10:01+60 > watermark 10:00:40


def test_duplicate_event_id_is_idempotent(client):
    batch = [ev("e1", "2026-09-22T10:00:05Z", 1.5)]
    ingest(client, "dev-1", batch)
    out = ingest(client, "dev-1", batch)  # 原样重发
    assert out["duplicates"] == 1 and out["accepted"] == 0

    windows = client.get("/v1/devices/dev-1/windows").json()["windows"]
    assert windows[0]["event_count"] == 1
    assert windows[0]["sum"] == 1.5


def test_watermark_seals_window_and_late_event_creates_correction(client):
    ingest(client, "dev-1", [ev("e1", "2026-09-22T10:00:05Z", 1.0)])
    # 推进水位线越过 10:01:00 -> 封存 [10:00, 10:01)
    ingest(client, "dev-1", [ev("e2", "2026-09-22T10:02:00Z", 9.0)])

    windows = client.get("/v1/devices/dev-1/windows").json()["windows"]
    w0 = next(w for w in windows if w["window_start"] == "2026-09-22T10:00:00Z")
    assert w0["status"] == "sealed"

    # 迟到数据落入已封存窗口 -> late + 修正记录，聚合值被修正
    out = ingest(client, "dev-1", [ev("e3", "2026-09-22T10:00:30Z", 5.0)])
    assert out["late"] == 1 and out["corrections"] == 1
    assert out["results"][0]["status"] == "late"
    assert out["results"][0]["window_status"] == "sealed"

    w0 = client.get("/v1/devices/dev-1/windows").json()["windows"][0]
    assert w0["event_count"] == 2 and w0["sum"] == 6.0 and w0["corrections"] == 1

    corrections = client.get("/v1/devices/dev-1/corrections").json()
    assert len(corrections) == 1
    assert corrections[0]["event_id"] == "e3"
    assert corrections[0]["value"] == 5.0


def test_late_event_does_not_advance_watermark(client):
    ingest(client, "dev-1", [ev("e1", "2026-09-22T10:00:05Z", 1.0)])
    ingest(client, "dev-1", [ev("e2", "2026-09-22T10:02:00Z", 9.0)])
    out = ingest(client, "dev-1", [ev("e3", "2026-09-22T10:00:30Z", 5.0)])
    assert out["watermark"] == "2026-09-22T10:01:30Z"  # 仍由 e2 决定


def test_event_before_watermark_without_existing_window_is_late(client):
    # 第一条事件就把水位线推到 10:01:30，旧窗口从未创建过
    ingest(client, "dev-1", [ev("e1", "2026-09-22T10:02:00Z", 9.0)])
    out = ingest(client, "dev-1", [ev("e2", "2026-09-22T10:00:10Z", 1.0)])
    assert out["late"] == 1 and out["corrections"] == 1

    w0 = client.get("/v1/devices/dev-1/windows").json()["windows"][0]
    assert w0["status"] == "sealed" and w0["corrections"] == 1


def test_within_allowed_lateness_is_accepted(client):
    ingest(client, "dev-1", [ev("e1", "2026-09-22T10:01:00Z", 9.0)])
    # watermark = 10:00:30，事件 10:00:20 落后于水位线但窗口未封存 -> 正常接收
    out = ingest(client, "dev-1", [ev("e2", "2026-09-22T10:00:20Z", 1.0)])
    assert out["accepted"] == 1 and out["late"] == 0


def test_window_status_filter_and_device_404(client):
    ingest(client, "dev-1", [ev("e1", "2026-09-22T10:00:05Z", 1.0)])
    ingest(client, "dev-1", [ev("e2", "2026-09-22T10:02:00Z", 9.0)])

    sealed = client.get("/v1/devices/dev-1/windows?status=sealed").json()["windows"]
    assert len(sealed) == 1 and sealed[0]["status"] == "sealed"
    open_ = client.get("/v1/devices/dev-1/windows?status=open").json()["windows"]
    assert len(open_) == 1 and open_[0]["status"] == "open"

    assert client.get("/v1/devices/nope").status_code == 404
    assert client.get("/v1/devices/nope/windows").status_code == 404


def test_watermark_persists_across_restart(tmp_path):
    db_path = str(tmp_path / "persist.db")
    settings = Settings(db_path=db_path, window_size_seconds=60,
                        allowed_lateness_seconds=30)
    c1 = TestClient(create_app(settings))
    ingest(c1, "dev-1", [ev("e1", "2026-09-22T10:02:00Z", 9.0)])

    # 模拟重启：同一 DB 文件重新建 app
    c2 = TestClient(create_app(settings))
    device = c2.get("/v1/devices/dev-1").json()
    assert device["watermark"] == "2026-09-22T10:01:30Z"
    # 重启后重复 event_id 仍被去重
    out = ingest(c2, "dev-1", [ev("e1", "2026-09-22T10:02:00Z", 9.0)])
    assert out["duplicates"] == 1
