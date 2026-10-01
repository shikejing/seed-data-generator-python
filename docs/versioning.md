# 算法版本策略

## 核心承诺

**生成算法会持续迭代，但已创建的定义必须永远复现创建当时的结果。**

为此，每个定义在创建时都会把算法版本**持久化钉住**
（`GenerationDefinition.algorithm_version`）。生成时随机域中包含该版本号，
存储的规约指纹中也包含它，因此：

- 升级程序、注册新版本，都不会改变任何已有定义的输出；
- 即使全局 `DEFAULT_VERSION` 在未来被提升，旧定义仍然按旧版本生成
  （测试 `test_new_version_default_does_not_change_old_definitions` 模拟了这一点）。

## 版本注册表

见 `seedgen/versions.py`：

| 常量 | 含义 |
| --- | --- |
| `DEFAULT_VERSION` | 新建定义默认使用的版本（当前 `1.0.0`） |
| `CURRENT_VERSION` | 本构建携带的最新版本（当前 `1.1.0`，示例预留） |

- 新版本通过 `register_version()` 注册；
- 创建定义时可显式传 `algorithm_version` 选择版本；
- 未注册的版本会被拒绝（`version_not_found`）；
- **新版本默认不生效**：提升 `DEFAULT_VERSION` 是一次有意识的发布动作，
  且它只影响此后新建、且未显式指定版本的定义。

## 如何发布一个改变输出的新版本

1. 不要改动任何旧版本字段生成器的字节行为；
2. 在 `versions.py` 注册新版本号（如 `1.2.0`）；
3. 让生成器按 `definition.algorithm_version` 分支实现新行为；
4. 需要切换默认时，单独修改 `DEFAULT_VERSION` 并发版；
5. 老定义继续钉在老版本上。

## 切换影响分析

在真正切换前，可以查询哪些定义会受到影响（只读操作，**不修改任何钉住版本**）：

```
GET /versions/1.1.0/impact
Authorization: Bearer <token>

200 OK
{
  "new_version": "1.1.0",
  "affected_definition_ids": ["def_abc...", ...],
  "count": 12
}
```

`affected_definition_ids` 是所有当前钉住版本 ≠ 目标版本的定义。
注意：由于旧定义永远按自身钉住的版本生成，这里的「受影响」指
*若将默认切换到新版本，未来新建定义将采用新版本*，而非旧数据会变化——
旧数据在任何情况下都不会变化。

## 字段兼容性矩阵（1.0.0）

| kind | 参数 | 说明 |
| --- | --- | --- |
| `int` | `min=0`, `max=2^31-1` | 闭区间整数 |
| `float` | `min=0.0`, `max=1.0`, `decimals` | 可定点保留位数 |
| `choice` | `options`（非空） | 从列表中等概率选取 |
| `bool` | `true_prob=0.5` | 伯努利 |
| `sequence` | `length`, `of={kind, params}` | 元素互相域隔离的列表 |
| `uuid` | — | 确定性 UUIDv4 形态字符串 |
| `text` | `words`, `length=4` | 从固定词库取词 |
| `name` | — | 固定姓名库拼接 |
| `email` | `domain="example.com"` | 确定性邮箱 |
| `date` | `start`, `end` | ISO 日期 |
| `timestamp` | `start`, `end` 或 `value` | Unix 秒 |

> 约束：所有生成器都不得引入依赖环境/时钟的新行为。新增字段类型时，
> 必须在新版本下引入，保证旧版本字节冻结。
