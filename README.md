# Seed Data Generator（可复现的种子化数据生成服务）

一个确定性、可复现、带权限控制与审计的种子数据生成服务。
**同一份定义 + 同一个种子，任何时间、任何机器、任何进程，输出逐字节一致。**

- 结果只取决于「生成定义 + 种子 + 钉住的算法版本」，
  不受系统时间、环境变量、字典/集合遍历顺序、运行时随机化影响；
- 一份定义含多个具名数据流，流之间结构性隔离；新增流/字段不影响既有输出；
- 算法版本随定义持久化钉住，新版本默认不生效、需显式选择，可做切换影响分析；
- 并发请求与重复提交收敛为同一份结果，原子落盘，无半成品；
- 支持分页与流式取用，大数据不整体进内存；取消/超时不留可见中间产物；
- 仅创建者或被授权角色可读取/重新生成，拒绝原因可区分，行为全量哈希链审计；
- 进程重启后可重新校验，缓存损坏/丢失自动检测并按相同字节重建。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload
# 或 uv run python main.py
```

内置演示令牌（仅用于本地/测试，生产应注入自己的令牌表）：

| Token | 用户 | 角色 |
| --- | --- | --- |
| `token-alice` | alice | generator |
| `token-bob` | bob | generator |
| `token-admin` | admin | admin |

### 1) 创建生成定义（钉住算法版本）

```bash
curl -s -X POST localhost:8000/definitions \
  -H "Authorization: Bearer token-alice" -H "Content-Type: application/json" \
  -d '{
    "name": "demo",
    "seed": "reproducible-42",
    "algorithm_version": "1.0.0",
    "shared_with": ["user:bob", "role:auditor"],
    "streams": [
      {"name": "users", "row_count": 1000, "fields": [
        {"name": "id", "kind": "int", "params": {"min": 1, "max": 100000}},
        {"name": "email", "kind": "email"},
        {"name": "level", "kind": "choice", "params": {"options": ["bronze","silver","gold"]}}
      ]},
      {"name": "orders", "row_count": 500, "fields": [
        {"name": "oid", "kind": "uuid"},
        {"name": "created_at", "kind": "timestamp",
         "params": {"start": "2023-01-01", "end": "2024-01-01"}}
      ]}
    ]
  }'
```

相同内容的重复提交返回 `200`（幂等命中），首次返回 `201`。
`idempotency_key` 用于客户端防重放；同 key 不同内容返回 `409`。

### 2) 触发生成（幂等，已生成则命中缓存）

```bash
curl -X POST ".../definitions/<id>/streams/users/generate" \
  -H "Authorization: Bearer token-alice"
```

### 3) 分页 / 流式读取

```bash
curl ".../definitions/<id>/streams/users/rows?offset=0&limit=100" -H "Authorization: Bearer token-alice"
curl ".../definitions/<id>/streams/users/stream" -H "Authorization: Bearer token-alice"   # NDJSON
```

### 4) 校验缓存 / 版本影响分析

```bash
curl ".../definitions/<id>/verify" -H "Authorization: Bearer token-alice"
curl "/versions/1.1.0/impact"      -H "Authorization: Bearer token-alice"
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/versions` | 版本列表（default/current） |
| GET | `/versions/{ver}/impact` | 切换默认版本的只读影响分析 |
| POST | `/definitions` | 创建/幂等提交定义 |
| GET | `/definitions` | 列出当前主体可见的定义 |
| GET | `/definitions/{id}` | 获取定义 |
| POST | `/definitions/{id}/streams/{s}/generate` | 幂等生成（可 `?timeout=`） |
| GET | `/definitions/{id}/streams/{s}/rows` | 分页 `offset/limit` |
| GET | `/definitions/{id}/streams/{s}/stream` | NDJSON 流式读取 |
| GET | `/definitions/{id}/verify` | 每流缓存校验状态 |

错误体统一为 `{"error": <code>, "message": ..., "reason"?: ...}`，
常用 code：`unauthorized(401)`、`forbidden(403)`、`validation_error(400)`、
`definition_not_found(404)`、`stream_not_found(404)`、
`idempotency_conflict(409)`、`cache_missing(409)`、
`generation_timeout(499)`、`generation_cancelled(499)`。

## 配置项（仅影响运维，不影响生成字节）

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `SEEDGEN_DATA_DIR` | `./data` | 定义、流数据、审计日志的根目录 |
| `SEEDGEN_DEFAULT_PAGE_SIZE` | `100` | 分页默认页大小 |
| `SEEDGEN_MAX_PAGE_SIZE` | `10000` | 单页上限（超出截断） |
| `SEEDGEN_DEFAULT_ALGORITHM_VERSION` | `1.0.0` | 新建定义的默认版本 |
| `SEEDGEN_GENERATION_TIMEOUT_SECONDS` | `30.0` | 生成接口默认超时 |

## 数据布局

```
<data_dir>/
  definitions/<definition_id>.json
  streams/<definition_id>/<stream>.jsonl
  streams/<definition_id>/<stream>.jsonl.meta.json
  audit.log
```

## 测试

```bash
uv run pytest
```

覆盖：确定性（含真实跨进程、不同 `PYTHONHASHSEED`）、多流隔离、版本兼容、
并发幂等（含高并发与「只生成一次」计数）、取消与超时、分页/流式内存、
缓存损坏/丢失/截断/规约失配的检测与同字节重建、权限与审计。

## 深入文档

- [复现语义](docs/reproducibility.md)
- [算法版本策略](docs/versioning.md)
