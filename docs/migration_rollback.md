# 数据迁移与回滚

所有新控制面数据库都使用新的 SQLite 文件，不覆盖原来的 `price_changes.db`、K 线数据库、模型或结果 JSON。

| 数据库 | 默认位置 | 创建者 | 原数据影响 |
|---|---|---|---|
| 预测控制面 | `ai_bot3/ai_bot3/data/control_plane.sqlite3` | `ControlPlaneRepository` | 无 |
| PIT 特征库 | 由配置指定 | `PointInTimeFeatureStore` | 无 |
| 慢研究任务 | `ai_bot3/ai_bot3/data/research_jobs.sqlite3` | `ResearchJobStore` | 无 |
| 交易执行状态 | `BybitContractBotV4/execution_state.sqlite3` | `ExecutionStore` | 无 |
| 重建特征候选 | `ai_bot3/ai_bot3/data/kline_feature_store.rebuilt.20260822.sqlite3` | 非破坏重建脚本 | 源库只读、生产路径未切 |

回滚顺序：

1. 保持交易端 `shadow` 并停止新票消费。
2. 备份对应的新 SQLite 文件及其 `-wal`、`-shm` 文件。
3. 若只是回退代码，保留数据库即可；新代码迁移均为 `CREATE TABLE IF NOT EXISTS`。
4. 只有明确决定删除新控制面数据时，才在备份副本确认后执行相应 `*_down.sql`。

特征库优先通过环境变量 `AI_BOT_KLINE_FEATURE_STORE_PATH` 显式选择，也兼容 `config.yml` 的 `general.kline_feature_store_path`。切换候选时不覆盖旧库，只改路径并以 shadow 重启；回滚时把路径恢复为 `./data/kline_feature_store.sqlite3`。旧库已确认损坏，所以回滚只能用于证据对照/停止服务，不能绕过完整性门禁继续训练。

回滚脚本不会由服务自动执行，避免误删。旧数据库和旧模型没有自动删除路径。
