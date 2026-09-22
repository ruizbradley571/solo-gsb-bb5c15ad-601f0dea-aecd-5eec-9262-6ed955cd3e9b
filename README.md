# Sensor Telemetry Aggregation API

基于 **FastAPI + SQLite** 的传感器遥测汇聚服务，支持 Docker 一键启动。

核心能力：

- **按设备的事件时间水位线（watermark）**：`watermark = max(已见 event_time) - allowed_lateness`，随每批数据推进并**持久化在 SQLite** 中，重启不丢失。
- **允许迟到窗口**：水位线越过窗口右边界（`window_end <= watermark`）时窗口被**封存（sealed）**；此前到达的数据正常计入。
- **幂等去重**：`event_id` 全局唯一，重复提交（重试/重发）不会产生重复计数。
- **迟到数据修正**：窗口封存后到达的数据标记为 `late`，写入**修正记录（corrections）**，并回改该窗口的聚合值与修正次数。
- **窗口聚合查询**：按设备返回每个时间窗的 `count / sum / avg / min / max`、状态（`open` / `sealed`）与修正次数。

## 快速启动

### Docker（推荐）

```bash
docker compose up --build
# 或
docker build -t telemetry-api .
docker run -p 8000:8000 -v telemetry-data:/data telemetry-api
```

服务监听端口：**8000**（宿主机映射同端口）。SQLite 数据文件落在容器 `/data/telemetry.db`，通过 volume 持久化。

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `WINDOW_SIZE_SECONDS` | `60` | 滚动窗口大小（秒） |
| `ALLOWED_LATENESS_SECONDS` | `30` | 允许迟到时长（秒），决定水位线偏移 |
| `DB_PATH` | 容器内 `/data/telemetry.db` | SQLite 文件路径 |

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/healthz` | 健康检查 |
| `POST` | `/v1/ingest` | 按设备提交事件批次（幂等） |
| `GET` | `/v1/devices/{device_id}` | 设备状态（水位线等） |
| `GET` | `/v1/devices/{device_id}/windows?status=open\|sealed` | 窗口聚合查询 |
| `GET` | `/v1/devices/{device_id}/corrections` | 修正记录列表 |

交互式文档：启动后访问 `http://localhost:8000/docs`。

## 示例请求

### 1. 提交一个批次

```bash
curl -X POST http://localhost:8000/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "sensor-001",
    "events": [
      {"event_id": "e-1", "event_time": "2026-09-22T10:00:05Z", "value": 21.5},
      {"event_id": "e-2", "event_time": "2026-09-22T10:00:35Z", "value": 22.0}
    ]
  }'
```

响应（每条事件给出接收状态与所属窗口）：

```json
{
  "device_id": "sensor-001",
  "accepted": 2, "late": 0, "duplicates": 0, "corrections": 0,
  "watermark": "2026-09-22T10:00:05Z",
  "results": [
    {"event_id": "e-1", "status": "accepted", "window_start": "2026-09-22T10:00:00Z", "window_status": "open"},
    {"event_id": "e-2", "status": "accepted", "window_start": "2026-09-22T10:00:00Z", "window_status": "open"}
  ]
}
```

### 2. 推进水位线，封存窗口

```bash
curl -X POST http://localhost:8000/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{"device_id": "sensor-001", "events": [
        {"event_id": "e-3", "event_time": "2026-09-22T10:02:00Z", "value": 23.7}]}'
```

`watermark` 推进到 `10:01:30`，窗口 `[10:00, 10:01)` 被封存。

### 3. 迟到数据 → 标记 late 并生成修正记录

```bash
curl -X POST http://localhost:8000/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{"device_id": "sensor-001", "events": [
        {"event_id": "e-4", "event_time": "2026-09-22T10:00:50Z", "value": 20.9},
        {"event_id": "e-1", "event_time": "2026-09-22T10:00:05Z", "value": 21.5}]}'
```

`e-4` 落入已封存窗口 → `"status": "late"`，生成一条修正记录并回改窗口聚合；`e-1` 为重复提交 → `"status": "duplicate"`，幂等忽略。

### 4. 查询窗口聚合

```bash
curl http://localhost:8000/v1/devices/sensor-001/windows
```

```json
{
  "device": {"device_id": "sensor-001", "watermark": "2026-09-22T10:01:30Z", ...},
  "windows": [
    {
      "window_start": "2026-09-22T10:00:00Z",
      "window_end":   "2026-09-22T10:01:00Z",
      "status": "sealed",
      "event_count": 3, "sum": 64.4, "avg": 21.47,
      "min": 20.9, "max": 22.0,
      "corrections": 1,
      "sealed_at": "2026-09-22T02:16:00Z"
    },
    {"window_start": "2026-09-22T10:02:00Z", "status": "open", "event_count": 1, ...}
  ]
}
```

### 5. 查询修正记录

```bash
curl http://localhost:8000/v1/devices/sensor-001/corrections
```

```json
[
  {"id": 1, "device_id": "sensor-001", "window_start": "2026-09-22T10:00:00Z",
   "event_id": "e-4", "event_time": "2026-09-22T10:00:50Z", "value": 20.9,
   "created_at": "2026-09-22T02:16:00Z"}
]
```

## 语义说明

- **批次是原子的**：同一批内的事件按提交顺序处理，整批在一个事务中完成；水位线推进与窗口封存在批末统一执行。
- **水位线只由正常接收的事件推进**：`late` 与 `duplicate` 事件不影响水位线。
- **迟到但窗口未封存**：事件时间落后于当前水位线、但所属窗口尚未封存时，仍正常计入（允许迟到窗口的意义）。
- **窗口从未出现但水位线已越过**：直接以 `sealed` 状态创建该窗口，事件记为 `late` 并生成修正记录。
- **`event_time` 缺省时区时按 UTC 处理**；响应中的时间均为 ISO 8601 UTC。

## 项目结构

```
app/
  config.py    # 环境变量配置
  db.py        # SQLite 连接与 schema
  schemas.py   # Pydantic 请求/响应模型
  service.py   # 水位线 / 窗口 / 幂等 / 修正核心逻辑
  main.py      # FastAPI 路由与应用工厂
tests/
  test_api.py  # 9 个端到端测试（含重启持久化用例）
Dockerfile
docker-compose.yml
requirements.txt
```
