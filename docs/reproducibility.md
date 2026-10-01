# 复现语义（Reproducibility Semantics）

本服务保证：**同一份生成定义 + 同一个种子，在任何时间、任何机器、任何进程中，
生成结果逐字节一致。**

## 1. 结果只由什么决定

某个数据流某一行的某个字段，唯一由以下输入决定：

| 输入 | 说明 |
| --- | --- |
| `seed` | 创建定义时给定的字符串种子 |
| 定义名 `name` | 稳定的命名空间标识 |
| 算法版本 `algorithm_version` | 定义创建时**钉住（pinned）**的版本 |
| 流名 `stream` | 具名数据流 |
| 行号 `row_index` | 从 0 开始的行下标 |
| 字段名 `name` + 字段类型 `kind` + 参数 `params` | 字段规约 |

以下因素**永不**参与生成：

- 系统时钟、`created_at` 等时间戳（时间戳只用于审计，不进随机域）；
- 环境变量（环境变量只能配置数据目录、分页大小、超时等**运维项**）；
- `dict` / `set` 的遍历顺序（所有序列化都 `sort_keys=True`，
  并有 `PYTHONHASHSEED` 无关性测试覆盖）；
- Python 的 `random` / `seeds` / 运行时 ASLR 等随机化。

随机源是自实现的 **SHA-256 计数器构造**（见 `seedgen/prng.py`）：

```
block_i = SHA256(SHA256("seedgen-rng/v1/" || domain_key) || row_index || purpose || counter)
```

- `domain_key = canonical_json([definition_name, algorithm_version, stream_name, seed])`
- 每个**字段**再以字段名做一次域隔离；
- `sequence` 的每个元素以 `字段名 + 元素下标` 再隔离。

因此：随机访问第 N 行与从头流式生成第 N 行完全一致；字段声明顺序、
兄弟字段消耗了多少随机数，都不影响某个字段的值。

## 2. 多流隔离

流与流之间在随机域上结构性隔离：某个流的 RNG 看不到任何其他流的名字或行数。

- 只请求 B 流，A 流结果不变；
- 请求顺序不同，结果不变（有跨目录、反序请求的测试）；
- **新增数据流不会改动已有数据流的输出**。定义 ID 虽然是整份文档（含所有流）
  的指纹，但它**故意不**进入随机域，也不进入单个流的缓存指纹；
- 同理，向一个流新增字段不会改变既有字段的值。

## 3. 分页与流式

- `GET .../rows?offset=&limit=` 分页读取；
- `GET .../stream` 以 `application/x-ndjson` 逐行流式输出；
- 生成时逐行写临时文件，大结果不会整体堆在内存里；
- 分页结果与独立重算逐行一致（有属性测试覆盖）。

## 4. 取消与超时

- 生成在每行之间检查取消信号；取消/超时后临时文件被删除并**原子改名回滚**，
  读者永远只能看到「旧的完整文件」或「新的完整文件」，看不到半成品；
- HTTP 层超时（默认见配置项）在后台线程生成时 `join(timeout)`，超时则
  置位取消事件，返回 `499 generation_timeout`，不落任何可见中间产物；
- 取消后再次请求可正常生成。

## 5. 缓存完整性与重建

每个流落盘为两个文件：

```
<data>/streams/<definition_id>/<stream>.jsonl
<data>/streams/<definition_id>/<stream>.jsonl.meta.json
```

`meta` 记录：行数、数据文件 SHA-256、规约指纹（spec fingerprint）。

每次读取/生成前都会校验三者：

- 校验和不符（追加、字节翻转）、行数不符（截断）、meta 损坏/丢失、
  数据文件丢失、规约指纹不符 —— 一律判定缓存损坏/陈旧，**删除并自动重建**，
  重建结果与原文件逐字节一致；
- 绝不会悄悄返回错误数据。`.../verify` 可显式查看每个流的校验状态。

## 6. 并发与幂等

- 每个 `(定义, 流)` 一把锁：并发/重复请求串行化，第一个生成并提交，
  其余命中缓存，**重复提交不会重复生成**（有计数测试断言只生成一次）；
- 定义创建有全局临界区，并发提交相同内容收敛为同一份定义（仅一次 `created=true`）；
- 写文件采用 `临时文件 + fsync + os.replace` 原子提交；
- 客户端可带 `idempotency_key`；同一 key 复放不同内容返回
  `409 idempotency_conflict`。

## 7. 访问控制与审计

- Bearer Token 鉴权；
- 仅**创建者**、`admin` 角色、或定义 ACL（`shared_with`，支持
  `user:<id>` / `role:<name>`）中被授权者可读取与重新生成；
- 越权返回可区分原因：`unauthorized`（无/错令牌）、
  `forbidden/not_creator_or_owner`（已认证但无权）、
  `forbidden/generator_role_required`（无创建角色）等；
- 所有创建、生成、读取、拒绝事件写入只追加的**哈希链审计日志**
  （每条记录含前一条哈希），可检测删除/篡改：`audit.verify_chain()`。
