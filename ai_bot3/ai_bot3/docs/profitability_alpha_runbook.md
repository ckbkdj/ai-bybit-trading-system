# Profitability-First Alpha v2：运行与恢复手册

> 本手册默认只运行研究、shadow 和 public market-data capture。不要启用 Bybit live，不要修改主网双开关。

## 1. 目录和环境

当前开发工作区：

```text
D:\Money
```

参考数据服务只读根目录：

```text
D:\lh\trad_data_service_20260821\data_service
```

进入项目目录：

```powershell
Set-Location D:\Money\ai_bot3\ai_bot3
```

使用项目锁定依赖；生产部署应换成正式 venv，但版本必须与锁文件一致：

```powershell
$env:PYTHONPATH='D:\Money\.test-deps312'
```

API key 只放进环境变量或机器本地 `.env.local`。报告、命令行输出、Git 和 trial ledger 都不能出现 key。

## 2. 运行前安全检查

必须确认：

1. 当前 Git 分支为 `codex/complete-profitability-alpha-v2`；
2. PR #4 仍是 Draft；
3. Bybit live 未启用，主网双开关未修改；
4. 旧 lockbox 没有被重新使用；
5. D 盘有足够空间；
6. 没有多个 archive backfill 同时写同一 SQLite；
7. 实验使用的 Bybit/macro observation sequence 已冻结；
8. 运行 commit 与 `--code-commit` 一致。

测试：

```powershell
python -m pytest -q
```

测试通过只说明程序回归通过，不说明盈利门禁通过。

## 3. FRED / ALFRED PIT 回填

只读调用官方 API，保存脱敏 descriptor、原始响应和 SHA-256：

```powershell
python scripts/backfill_fred_alfred_pit.py `
  --start 2018-01-01 `
  --end 2026-08-20 `
  --database data/macro_pit.sqlite3 `
  --cache-dir data/fred_alfred_cache `
  --env-file D:\lh\trad_data_service_20260821\data_service\.env.local `
  --report model_results/evaluation/fred_alfred_backfill_report.json
```

合格审计至少包括：

- 所有 HTTP status 为 200；
- raw file 长度和 SHA 与 response evidence 一致；
- request descriptor 不含 `api_key`；
- `event_time <= available_at <= ingested_at` 违规为 0；
- `current_snapshot_substitution=false`；
- output 4 和 output 3 分别保留初值与修订；
- VIXCLS/DFII10 超过官方 2000 vintage 上限时按 realtime window 分片。

FOMC Tier-A 必须另行抓取美联储官方声明，不能由 FRED vintage 或会议计划日期替代：

```powershell
python scripts/backfill_fomc_pit.py `
  --start 2018-01-01 `
  --end 2026-06-18 `
  --database data/macro_pit.sqlite3 `
  --cache-dir data/fomc_pit_cache `
  --report model_results/evaluation/fomc_pit_backfill_report.json
```

审计必须逐份验证官方年度索引和声明正文 SHA-256、`For release at` 明示时间、EST/EDT 到 UTC 的换算、声明前不可见、24 小时复位，以及 minutes/implementation note 排除。页面没有明确 release time 时脚本必须失败关闭，禁止猜测。

## 4. Bybit public-only 实时采集

在启动 Bybit 采集前，可先回填无需密钥的稳定币链上发行 flow：

```powershell
python scripts/backfill_coinmetrics_stablecoin_pit.py `
  --start 2018-01-01 `
  --end 2026-08-20 `
  --database data/flow_pit.sqlite3 `
  --cache-dir data/coinmetrics_cache `
  --report model_results/evaluation/coinmetrics_stablecoin_backfill_report.json
```

审计必须确认原始响应长度/SHA 一致、时序违规为 0、descriptor 无 key，并明确这些列是 USDC/USDT 发行/赎回变化而不是 exchange netflow。任何后续抓取与既有历史值冲突都应失败关闭，不能覆盖旧证据。

数字资产投资产品周 flow 使用 CoinShares 官方历史发布物：

```powershell
python scripts/backfill_coinshares_fund_flow_pit.py `
  --start 2024-01-01 `
  --end 2026-08-20 `
  --database data/flow_pit.sqlite3 `
  --cache-dir data/coinshares_cache `
  --workers 4 `
  --report model_results/evaluation/coinshares_fund_flow_backfill_report.json
```

检查 sitemap 文章数、成功解析数、exclusions、发布日期唯一性、最大周度间隔、正负号和极值。累计/YTD/年度金额不得当成周流量；parser 修正必须新增版本和失效记录，不能删除旧错误证据。该列不是日度 issuer-level ETF creation/redemption，报告和模型卡中必须保留这一语义边界。

启动命令只允许公开线性行情 endpoint，不含认证或交易参数：

```powershell
python scripts/run_bybit_public_pit_collector.py `
  --database data/bybit_public_pit.sqlite3
```

启动输出必须显示：

```json
{
  "mode": "public_market_data_capture_only",
  "authentication": false,
  "trading": false
}
```

数据库中最新 `bybit_capture_sessions` 必须为：

- endpoint：`wss://stream.bybit.com/v5/public/linear`；
- status：`running`；
- error：`null`；
- sequence 持续增加。

如果进程存在但 session 不更新，先看 stderr 重连日志。不要把“PID 活着”当作采集成功。

采集器使用数据库 lease 防止同一 PIT 仓双开：新进程启动时会在立即写事务内检查 `running` session 的启动时间和最近 raw event。120 秒内仍活跃的 session 必须拒绝竞争进程，不能把它改成 `disconnected`；只有超过 stale threshold 的无活动 session 才可按非正常退出接管。健康检查要同时核对最新 raw event 的 `session_id` 与 session 状态为 `running`。

## 5. Bybit 官方历史 archive 回填

archive replay 与实时采集共用数据库时只能单进程运行。更推荐先停 public collector，完成一批 archive、checkpoint 后再恢复 collector。

```powershell
python scripts/backfill_bybit_historical_archive.py `
  --start 2026-07-15 `
  --end 2026-08-20 `
  --symbols BTCUSDT ETHUSDT XRPUSDT SOLUSDT 1000PEPEUSDT `
  --kinds orderbook trades `
  --database data/bybit_public_pit.sqlite3 `
  --cache-dir data/bybit_archive_cache `
  --emit-interval-sec 15 `
  --assumed-feed-latency-ms 1000 `
  --report model_results/evaluation/bybit_archive_backfill_report.json
```

脚本会跳过 `status=completed` 的 append-only manifest；失败项必须保留并在单进程重试，不能把 failed 改成 completed。

合格条件：

- 日期数 × 5 symbols × 2 kinds 全部 completed；
- failed=0；
- 每个 archive 的 source URL、content length、SHA、member、首末 event time、rows 和 feature count 完整；
- orderbook/trades 事件都属于对应 UTC trading date；
- archive replay 明确不是 live capture；
- 不使用 OHLCV 推测盘口。

## 6. Bybit 官方衍生品历史

使用官方 funding history、open-interest history、mark/index kline，保留每个请求响应哈希和 batch manifest。运行前后检查：

- funding 是已结算费率，不是未来预告；
- OI change 只用决策时已发布观测；
- basis 使用同时间的 mark/index；
- 任何 ret_code、HTTP、分页或 chronology 失败都会使 batch failed；
- liquidation 不从 OHLCV/REST 伪造。

### 6.1 连续性审计与爆仓证据交接

`feature_coverage.start/end` 只说明首末观测跨度，不能证明中间没有断流。正式短周期消融还必须满足：

- orderbook/trades/funding/OI/basis：每个 symbol-feature 至少 180 个连续 UTC 日都有 `completed` 官方 archive/API manifest；
- live 因子：封存的原始 WS 日志在 90 秒断流阈值下至少形成 180 天连续区间，并且 5 个 symbol 的所需 topic 都有原始事件；
- liquidation：只能使用后一种 forward-only WS 连续性证据；事件本身可以稀疏，不能把“某天无爆仓”误判成断流，也不能只看第一笔和最后一笔爆仓；
- 运行中的 session 不可封存，不可进入 release evidence。

满 180 天后先正常停止 public-only collector，确认没有 `status=running` 的 session，再做逐条 payload SHA 审计：

```powershell
python scripts/audit_bybit_live_capture.py `
  --database data/bybit_public_pit.sqlite3 `
  --maximum-gap-sec 90 `
  --report model_results/evaluation/bybit_live_capture_audit_report.json
```

历史 archive/API 与实时 capture 保持物理分库。待 archive 和 derivatives backfill 全部停止后，只把已封存的 liquidation 原始事件、v2 特征、失效记录、session 和连续区间收据追加进 development 仓：

```powershell
python scripts/merge_bybit_liquidation_capture.py `
  --source data/bybit_public_pit.sqlite3 `
  --destination data/bybit_public_pit.prelockbox.sqlite3 `
  --report model_results/evaluation/bybit_liquidation_capture_import_report.json
```

合并是幂等的：同一内容重复运行新增数为 0；同 ID 异内容立即失败。它不复制数 GB 无关 live orderbook/trades，也不删除或覆盖任一来源库。禁止在 archive/derivatives writer 仍写 destination 时运行。

启动 profitability trial 时记录的 Bybit 快照必须同时包含 feature sequence、invalidation rowid、capture audit rowid 和 import receipt rowid。实验启动后新增的 audit/import 不得进入该 trial；需要使用新证据时必须形成新 trial ID。不要手工 UPDATE/DELETE raw、feature、invalidation、completed archive/API batch、API response、audit、interval 或 import 表；数据库 trigger 会失败关闭。

## 7. SQLite WAL 安全恢复

### 7.1 何时处理

出现以下任一情况先停止大规模 archive writers：

- 多个 backfill 重叠；
- `database is locked` 持续增加；
- WAL 快速增长；
- 剩余空间不足以完成本批次。

### 7.2 原则

- 不手工删除有效 `-wal`；
- 先停止 archive writers；
- 必要时短暂停止 public-only collector；
- 使用 SQLite 自己执行 checkpoint；
- 验证已 checkpoint frame 数等于总 frame 数；
- 关闭最后连接后确认旧 WAL 由 SQLite 自动移除；
- 再恢复 collector，并验证新 running session 和 sequence 增长。

示例：

```powershell
python -c "import sqlite3; c=sqlite3.connect(r'data/bybit_public_pit.sqlite3', timeout=300); print(c.execute('pragma wal_checkpoint(PASSIVE)').fetchone()); c.close()"
```

不要在磁盘不足时运行会产生大型临时结构的全库 `quick_check`。优先使用 checkpoint 计数、关键表计数、随机范围读取、来源哈希和小批次验证；需要完整 integrity check 时先准备足够临时空间或复制到独立研究盘。

## 8. Development-only 盈利实验

在 development 阶段接入真实跨资产和 macro PIT；Bybit 历史未完整前可先省略 `--bybit-pit-store` 做中长周期研究，但短周期因子组会失败关闭：

`--trad-panel-root` 不是“给一个 Parquet 就接受”。该根目录必须同时具有 canonical、baseline、最近 PASS 发布收据和匹配 canonical SHA 的质量审计；before/after 哈希不一致、允许标的历史价格改写、旧日期回填或基础价格审计问题都会直接终止实验。当前参考服务最近一次更新任务为 `BLOCKED`，所以实时 provider 保持 `degraded`；即使某个正式模型将来保留跨资产组，运行时也会返回 `NO_TRADE`，直到数据服务恢复健康更新。

```powershell
python scripts/run_profitability_rebuild.py `
  --trad-panel-root D:\lh\trad_data_service_20260821\data_service `
  --macro-pit-store D:\Money\ai_bot3\ai_bot3\data\macro_pit.sqlite3 `
  --flow-pit-store D:\Money\ai_bot3\ai_bot3\data\flow_pit.sqlite3 `
  --bybit-pit-store D:\Money\ai_bot3\ai_bot3\data\bybit_public_pit.sqlite3 `
  --max-bars-per-symbol 200000 `
  --walk-forward-folds 3
```

退出码：

| 退出码 | 含义 |
|---:|---|
| 0 | 当前作用域全部门禁通过；仍需核对是否只是 development，不能直接 live |
| 1 | pipeline 异常，已写失败输出和 ledger |
| 2 | pipeline 完成但盈利/证据门禁失败，正确状态是 rejected |

每个 trial 必须在 `data/research_trials.sqlite3` 中保留 running、阶段事件和最终 rejected/candidate 记录。不要删除失败 trial。

大样本训练失败时先读取 ledger 的 `pipeline_error`。编码器应只构造一次特征矩阵并逐列原地标准化；ridge、direction 和 meta 优化不得为截距额外复制整块矩阵。Bybit loader 还必须先冻结 sequence，再按 development 决策窗和各因子最大陈旧期在 SQL 层裁剪；禁止为一个早于现有盘口历史的开发窗把千万级全库载入内存。每个 horizon 第一遍只读取时间和覆盖范围来封存边界，第二遍按 symbol 逐个工程化、标注并释放；Bybit join 随该 horizon 完成后立即释放快照。dataset 建成时逐个弹出并释放原 panel，禁止为了一个尚未获准打开的 lockbox 常驻保存 25 份完整历史。通过 development 后如需重建新 lockbox，必须先验证 K 线源 size/mtime 与 trial 冻结值完全一致。已用失败现场同形状的 85,128×40 float64 矩阵做回归验证。该验证只证明内存路径可执行，不是盈利证据。

长时消融必须持续写入 `factor_ablation_fold_progress`：每个 horizon/fold 至少有 `STARTED` 以及 `COMPLETED` 或 `SKIPPED_INSUFFICIENT_PIT_ROWS`，并记录 train/test 行数。只有进程 CPU、数据库 mtime 和 ledger 心跳都停止增长时才判定可能卡死，不能因单个真实大样本 fold 数小时未完成就擅自终止。

生产 shadow 推理若模型签名包含对应因子，必须同时配置：

```powershell
$env:BYBIT_PUBLIC_PIT_STORE='D:\Money\ai_bot3\ai_bot3\data\bybit_public_pit.sqlite3'
$env:MACRO_PIT_STORE='D:\Money\ai_bot3\ai_bot3\data\macro_pit.sqlite3'
$env:FLOW_PIT_STORE='D:\Money\ai_bot3\ai_bot3\data\flow_pit.sqlite3'
```

runtime 会校验模型和 bundle 哈希、逐列 staleness、`available_at <= decision_at`、原始响应长度/SHA 和 candidate manifest。任一条件不满足都只输出 `NO_TRADE`，不得由 Brain 或默认值补位。

## 9. 报告阅读顺序

按以下顺序审阅：

1. `factor_ablation_report.json`：真实数据、完整组、足量 trades、fold 改善；
2. `walk_forward_report.json`：outer OOS 是否从未参与调参；
3. `execution_cost_report.json`：费率、滑点、partial fill、latency、MTM；
4. `capital_preservation_report.json`：单笔、日/周损失、回撤、杠杆和止损；
5. `profitability_report.json`：development/lockbox 门禁总结果；
6. `lockbox_report.json`：未获授权时必须保持 sealed/unlabeled；
7. `candidate_release_manifest.json`：只有全部门禁通过时才允许存在。

失败时应明确看到：

```text
profitability_gate=FAILED
candidate_count=0
live_count=0
```

0 signals、0 trades、净收益 0、回撤 0 不是完成证据。

`profitability_report.json` 中还必须看到 `independent_return_clusters` 和
`bootstrap_expectancy_unit=utc_calendar_day_portfolio_net_return`。同日相关交易先聚合，
首末成交日之间的无交易日按零收益计入；不足 20 个日簇、缺少 UTC 退出时间，或
moving-block bootstrap 的 95% 下界不大于零，都不得通过。

`execution_cost_report.json` 必须分别核对：

- `official_pit_cost_inputs_complete`：每笔交易的 entry/exit spread、depth 和持仓 funding 都有官方 PIT provenance；
- `proxy_execution_cost_trade_count`：必须为 0；
- `shadow_or_testnet_fill_receipts_complete`：必须有独立 OOS 成交回执；
- `queue_position_and_latency_calibration_complete`：必须用这些回执完成校准。

官方历史盘口可以替换 OHLCV spread/depth 代理，但不能独自满足后两项。

此外必须检查 `walk_forward_report.json.direct_execution_release_datasets`：

- `selection_policy` 必须为 `full_maximum_execution_window_direct_before_outcome_filtering`；
- `selection_columns` 只能包含 `execution_window_evidence_complete`，不得读取 `net_return`、MAE/MFE 或 exit reason 来选样本；
- 完整路径必须由连续 K 线 `close_time` 覆盖；较晚的 funding `available_at` 不得被当成价格路径结束时间；
- `release_walk_forward_ready=true` 后，该周期才允许进入正式 walk-forward 和因子消融；
- direct 样本不足必须显示 blocker 并保持 rejected，禁止回退到 OHLCV 代理成本凑交易数。

blocker 只允许表示样本量、fold 或跨币种不足。PIT 违规、混周期、非法标签或 schema 损坏必须终止 trial 并登记 `pipeline_error`，不得伪装成普通的数据收集中状态。

## 10. 因子消融验收

每个 baseline/augmented arm 至少满足代码中预先锁定的 OOS trade/fold 下限。只有以下条件同时满足才 retained：

- 完整因子组没有 missing required factor；
- 相同 purged outer folds；
- folds 必须来自完整最大执行窗口均有直接成本证据的 release dataset；
- 相同成本、风险和事件回测；
- baseline 和 augmented 都有足量真实交易；
- 费用后平均改善、正改善 fold 比例和最差 fold 退化都过门槛。

研究用固定 2% OOS ranking 只用于测量因子增益。它可以让负 edge 信号进入“研究回测”，但不会改写真实 lower-bound edge，也不会放松生产 TRADE gate。

liquidation 组必须额外核对 `collection_evidence`。Bybit 官方公开的是 [`allLiquidation.{symbol}` 实时 WS](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation)，500ms 推送；当前 [V5 Market REST 清单](https://bybit-exchange.github.io/docs/api-explorer/v5/market/market)没有公共历史爆仓接口。因此 `historical_backfill_supported=false` 是真实来源限制，不是待补代码。达到 180 天 forward-only PIT 历史前，该组和总因子门禁保持失败。

## 11. Lockbox 操作

当前禁止打开新 lockbox。只有 development 报告全部通过后：

1. 固化 code commit、数据 SHA、sequence、参数和 trial ID；
2. 人工审批一次新 lockbox；
3. 只评分一次，不调参；
4. 结果无论好坏立即登记为 consumed；
5. 失败后回 development，但不能再次使用该 lockbox；
6. 通过后最多生成 candidate，仍不能 live。

## 12. Shadow、测试网和人工批准

candidate 后仍需：

- 长时间 shadow soak；
- 足量实际信号、订单意图、取消、partial fill 和结算证据；
- 2× 成本和延迟压力；
- 数据源中断、模型缺失、manifest 不一致、kill switch 演练；
- Bybit 测试网；
- 独立人工批准。

主网双开关不属于本研究 runner 的权限范围。

## 13. Git 和发布纪律

- 只在 `codex/complete-profitability-alpha-v2` 推送；
- PR #4 始终 Draft，未达到全部指标前不得合并；
- runtime 报告和本地数据库不提交；
- 源码、测试、文档分别使用精确文件列表暂存，避免带入用户运行产物；
- 每个重要证据提交后在 PR Conversation 留审计说明；
- 不删除旧模型、数据库、策略或失败记录。
