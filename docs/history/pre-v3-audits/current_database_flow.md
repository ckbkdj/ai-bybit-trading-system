# 当前数据库与持久化流程

审计日期：2026-08-21

## 总览

当前系统不是一个共享数据库，而是多组 SQLite、JSON、INI、模型文件与交易所远端状态共同组成。预测端和交易端没有共同的 transaction id，也没有从 forecast 到 order/fill 的血缘链。

## 预测端

| 存储 | 写入者 | 读取者 | 用途 | 主要缺口 |
|---|---|---|---|---|
| `data/{SYMBOL}.sqlite` | `DataFetcher._save_cache` | DataFetcher、在线学习结算 | 每个周期的 OHLCV 缓存 | `to_sql(... replace)` 整表替换；数值列当前为 TEXT；无 source/revision/available_at |
| `data/kline_feature_store.sqlite3` | `KlineFeatureStore` | 训练流程 | raw/enhanced K 线、模型注册 | 有 fetched/computed/version hash，但仍缺 published_at、available_at、revision vintage |
| `data/online_learning.sqlite3` | `OnlinePredictionCalibrator` | 校准与评估导出 | 预测、到期结算、命中和成本后收益 | 无 forecast_id；记录与最终 JSON 不能稳定一一对应 |
| `data/brain_training_history.sqlite3` | Brain 训练流程 | Brain 治理 | 训练运行历史 | 只覆盖 Brain 子模型，不是统一模型注册表 |
| `data/coinglass_metrics/*.json` | 外部采集/上下文代码 | 推理、API、情绪 | 衍生品、新闻、日历、鲸鱼等快照 | 多数是最新覆盖文件，无不可变事件修订链 |
| `model_results/{symbol}_{mode}.json` | `ResultManager` | API、交易端间接读取 | 最新预测 | 覆盖写；无 schema/revision/发布事务；主预测写入非原子替换 |
| `model_results/*_training.json` | Trainer/ResultManager | API | 训练元数据 | 与预测仅靠文件名关联 |
| `model_results/evaluation/*.json` | Online calibrator | API | 评估摘要 | 派生缓存，不是评估事实表 |
| `models/read|write|backup` | 训练/提升逻辑 | 推理 | 模型候选、正式和备份 | 本地目录约定，缺统一 bundle manifest |

### `kline_feature_store.sqlite3` 当前表

- `raw_kline`
- `enhanced_kline`
- `model_registry`
- `enhanced_update_meta`

这是最接近 point-in-time store 的现有基础，适合渐进扩展，不建议另起一套重复 K 线库。但它当前主要围绕 K 线和计算版本，不能直接代表宏观数据在历史时刻的真实可见性。

### `online_learning.sqlite3` 当前表

`predictions` 已含：symbol、timeframe、mode、预测收益、方向、置信度、模型版本、杠杆、结算时间、实际收益、成本后收益和命中等字段。

优点是使用增量 `ALTER TABLE ADD COLUMN`，没有破坏旧字段。缺点是没有稳定 forecast ID、feature snapshot identity 和发布 revision 的强约束。

## 交易端

| 存储 | 写入者 | 读取者 | 用途 | 主要缺口 |
|---|---|---|---|---|
| `setting_v4.ini` | 主交易脚本 | 主交易脚本 | 参数与运行状态混存 | 非事务、无版本、无并发控制、直接覆盖、无法审计 |
| `price_changes.db` | `earnings.py` | 收益通知 | 账户总值快照 | 只有 `price_changes(id,timestamp,price)`，没有订单/成交/PnL 事实 |
| Bybit 远端账户 | CCXT | 主交易脚本 | 余额、仓位、未完成订单 | 实际执行事实仅存在交易所；本地没有完整镜像 |
| `hot_db.py` / `cloud_db.py` | 当前主版本未使用 | 当前主版本未使用 | PostgreSQL 仓位实验代码 | 含连接配置和模块级测试调用；不应作为现有主链路依据 |

`hot_db.py` 和 `cloud_db.py` 没有被 `v4_1` 导入。两者在模块末尾存在导入即执行的测试写入，应在任何测试发现或自动导入前隔离。

## 当前数据所有权

```text
Binance/Coinglass -> ai_bot3 SQLite/JSON -> model_results JSON
                                           -> API /results
                                           -> v4_1 读取 trend

Bybit -> v4_1 轮询余额/仓位/订单
      -> setting_v4.ini 保存策略运行状态
      -> price_changes.db 保存账户总值快照
```

不存在反向回执链：Bybit fill 不会带 forecast ID 回到 ai_bot3，预测评估使用的是后续 K 线收益，不是真实成交、手续费、滑点和资金费率。

## 数据一致性风险

1. 预测 JSON 与交易 INI 各自覆盖，没有跨文件事务。
2. 相同预测可以被重复读取，系统没有消费记录。
3. INI 状态在订单成交前更新，可能与交易所事实分叉。
4. 账户总值数据库不能用于订单恢复或逐笔归因。
5. 当前数据库没有 migration version 或 rollback journal。
6. 两个项目均没有 Git 基线，代码和 schema 改动缺少可靠对照点。

## 阶段化演进建议

### 阶段 1

只新增 contracts 与 append-only shadow 表，不迁移旧数据：

- `forecast_revisions`
- `operation_tickets`
- `ticket_events`
- `execution_receipts`

旧 JSON/INI 继续作为 legacy source，适配器负责读取。

### 阶段 2

增加 outbox 和影子 consumer：

- 每张票唯一 `ticket_id`。
- consumer claim 和 lease。
- 重复提交只写重复事件，不产生第二次模拟订单。

### 阶段 3

增加真实执行账本，但仍保留 legacy fallback：

- `execution_orders`
- `execution_fills`
- `position_snapshots`
- `reconciliation_runs`

任何迁移都要先备份数据库、记录 schema version、提供向下迁移或停用新表的回退方式。现有数据库和模型文件不得删除。
