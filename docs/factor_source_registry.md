# 因子与数据源登记表

更新时间：2026-08-24

## 结论

当前系统并不是“几十个因子都已经在线生效”。能进入现有快速预测的，主要是历史 K 线派生技术量价、资金费率/多空比、本地 Coinglass 衍生结构文件、事件上下文和可选内网 LLM；订单簿、逐笔流、链上、宏观、跨资产、中国与医疗因子已有接口、语义和 PIT 框架，但没有部署方提供并验证的数据流时，必须保持缺失，不能用 0 冒充真实观测。

## 当前实际进入预测的因子

| 因子族 | 字段/计算 | 当前来源 | 时间要求 | 当前用途与限制 |
|---|---|---|---|---|
| OHLCV | open/high/low/close/volume | Binance Futures，经现有 `data_fetch`/CCXT 拉取并缓存 | K 线截止点不得晚于预测创建时刻；451 或缓存陈旧时失败关闭 | LSTM 和 Brain 的主要历史数据；预测交易所与执行交易所不一致，测试网前必须评估 basis/延迟 |
| 技术量价 | SMA、RSI、Bollinger、MACD、ATR、收益、波动、成交量 z-score、K 线实体/影线、趋势强度 | 只由上述历史 K 线在本机计算 | 所有输入至少 shift 1；标签只使用未来 horizon | 直接进入 LSTM/Brain；不是独立外部信息源 |
| 衍生品结构 | funding、funding acceleration、long/short ratio/change、OI/change/notional、taker ratio、top-trader ratio | 本地 `coinglass_metrics/*.json` 优先，关键 funding/long-short 可由 Binance Futures 回退 | 文件带 generated_at；关键值不可伪造中性值 | 当前快速融合使用；本地 Coinglass 采集链仍需在安全机上登记最终 URL、授权与 SLA |
| 爆仓结构 | long/short liquidation、imbalance、近端爆仓距离、热度、层数、lastPrice | 用户现有 `data/{BASE}.json` 和本地 Coinglass 指标文件 | 当前价默认最大 600 秒；交易票要求更严的 120 秒 | 作为结构因子；爆仓热区不等于必然支撑/阻力 |
| 事件/情绪 | events、news context、financial calendar、whale alert、fear & greed | `data/coinglass_metrics/*.json` 本地文件 | 每项独立 generated_at/完整度 | 只做低权重辅助；未验证来源不能触发 Tier A 事件封锁 |
| LLM 辅助 | 结构化 score、summary | 配置中的内网 OpenAI-compatible/Qwen 端点 | 请求只携带结构化快照；失败返回中性 | 仅为辅助因子，不能单独出票、调仓、绕过风险门禁 |
| 在线校准 | bias、scale、adaptive threshold、direction confidence | `online_learning.sqlite3` 中已到期预测 | 只有 settled 样本达到 min_samples 才是 `valid` | 现在会显式输出 `valid/insufficient_samples/disabled`；没有 valid 就不出票 |
| range guard | scaler 空间训练范围违例率与最大超界 | 当前 LSTM 输入和训练期 scaler | 每次推理 | 不再误称 OOD；它只是范围报警。缺 scaler 或非有限值时失败关闭 |
| 跨资产影子上下文 | SPY/QQQ/TLT/GLD/USO/UUP/GBTC/COIN 的 1/5/20 日收益 | `D:\lh\trad_data_service_20260821\data_service\data\canonical\panel.parquet`，只读四列 | `ts <= as_of-30h`，并校验最后 PASS 发布收据 SHA | 已真实跑通但 `fusion_eligible=false`；只做研究/影子记录，不影响方向或出票 |
| Bybit 执行快照 | mark/bid/ask、instrument rules、账户、仓位、订单、fill、服务器时钟 | Bybit V5 公共与私有接口 | 决策前/执行中实时核对 | 是最终执行真值；Binance 只是预测市场，短周期必须有 basis 证据 |

## 已实现计算，但尚未证明为在线生产输入

| 因子族 | 已有能力 | 推荐第一方/权威源 | 当前状态 |
|---|---|---|---|
| Bybit 订单簿 | spread、L5 delta/imbalance/depth、microprice、滚动 OFI、top-5 扫单完成比例与 VWAP slippage | Bybit 官方历史归档 + V5 public orderbook snapshot/delta，校验 `u/seq` 并重建每次 L2 状态 | 归档回放和实时 collector 已实现；一日真实文件验算通过，正式多币种覆盖、消融和成交回执仍未完成，不进入真钱模型 |
| Bybit 主动成交 | aggressive buy/sell、滚动 CVD、成交笔数和名义 | Bybit 官方历史归档 + V5 public trade | 归档回放和实时 collector 已实现；一日真实文件验算通过，正式覆盖和消融未完成 |
| Bybit funding/OI/basis | 实际结算 funding、严格向后 1 小时 OI 变化、1 分钟 mark/index basis | Bybit V5 funding history、open-interest、mark/index price kline | 官方 REST 日批次回放已实现；一日真实响应验算为 3/288/1,440 条，正式多币种覆盖与消融未完成 |
| 链上交易所流 | stablecoin/coin exchange netflow、确认数、标签修订风险 | 经批准的专业链上数据供应商或自建节点+版本化标签 | 数据结构已实现；无已批准 provider |
| 美元流动性/宏观 | Fed balance sheet、RRP、TGA、实际利率、DXY、信用、增长/通胀 surprise | FRED/ALFRED vintage；美国财政部/纽约联储；EIA 等官方源 | PIT/vintage 存储和状态聚合已实现；未配置数据流 |
| 监管/公司事件 | 申报、监管公告、重要公司文件 | SEC EDGAR API 和各监管机构第一方发布 | 研究任务/source tier 已实现；未配置数据流 |
| 跨资产 | 黄金、原油、美股、风险偏好、美元、中国、医疗轮动 | 交易所/官方/已授权专业行情 | 只允许外部训练后的 regime 权重；无校准权重时函数拒绝运行 |

参考面板虽然含 5,332 列，但 `asset_family` 有历史错标/漂移，故禁止按分类标签批量取因子，也禁止把全部字段直接输入模型。当前适配器只按显式 symbol 白名单读取基础收盘价，标签只输出供审计。

Bybit 历史归档适配器会登记原始 URL、文件 SHA-256、事件范围、读取行数、派生特征数和 `historical_archive_replay` 来源，并强制 `event_time <= available_at <= ingested_at`。2026-08-01 的 1000PEPEUSDT 官方文件隔离验算读取 528,558 条订单簿事件和 118,194 条成交，PIT 时间违规为 0。这个结果只证明真实来源和回放正确性，不等于足量 OOS 数据或 execution evidence：top-5 深度扫单完成比例不是 maker 排队成交概率，真实 fill/partial fill 仍必须来自 shadow/testnet ExecutionReceipt。

衍生品 REST 历史回放使用独立的 `historical_api_replay` 来源。每个日批次保留请求 URL、响应体 SHA-256、行数、请求/接收时间和请求清单哈希；basis 的一分钟收盘值只在 K 线结束并增加保守延迟后可用，OI 变化只引用一小时前已存在的观测。2026-08-01 的 1000PEPEUSDT 实测合计 1,731 个特征、7 个官方响应、PIT 时间违规为 0。Bybit 官方公开接口仍未提供可追溯的历史 liquidation REST 数据，所以不得用 OHLCV、普通成交或 OI 变化伪造爆仓流。

权威接口：

- [Bybit Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)、[Public Trades](https://bybit-exchange.github.io/docs/v5/websocket/public/trade)、[Funding History](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate)、[Open Interest](https://bybit-exchange.github.io/docs/v5/market/open-interest)、[Mark Price Kline](https://bybit-exchange.github.io/docs/v5/market/mark-kline)、[Index Price Kline](https://bybit-exchange.github.io/docs/v5/market/index-kline)、[Instrument Info](https://bybit-exchange.github.io/docs/v5/market/instrument)
- [FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)、[vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html)
- [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)、[EIA Open Data](https://www.eia.gov/opendata/index.php/api)
- [Binance USDⓈ-M Market Data](https://developers.binance.com/en/docs/derivatives/usds-margined-futures/market-data/rest-api)：K 线可分页；OI history 仅约 1 个月，long/short 与 taker history 仅约 30 天，不能从当前官方接口恢复完整三年历史。

## 来源等级

- Tier A：交易所第一方、政府/监管官方发布；可在验证后参与 blackout 确认。
- Tier B：有合同/SLA 的专业数据；可形成因子，但不能单独确认重大事件。
- Tier C：新闻、社媒、搜索和 LLM；只用于发现与弱辅助。

任何 provider 都必须登记 owner、endpoint、证书/CA、授权范围、时区、event/published/available/ingested 时间、修订策略、限流、陈旧阈值、缺失语义和回退策略。未登记或过期就是 missing/degraded，不补假值。
