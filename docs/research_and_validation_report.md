# 市场研究、过拟合控制与验证报告

更新时间：2026-08-23

## 市面研究得到的产品方向

- 加密资产存在市场、规模和动量等横截面共同因子，但不能直接把论文结论当作本系统 alpha。[NBER：Common Risk Factors in Cryptocurrency](https://www.nber.org/papers/w25882)
- 动量和投资者注意力在加密收益中有解释力，但宏观与传统资产关系会随样本和 regime 改变，不能写死黄金、美股或美元与 BTC 的符号。[NBER：Risks and Returns of Cryptocurrency](https://www.nber.org/papers/w24877)
- 深度订单簿模型证明 LOB 具有可学习结构，但模型表现依赖撮合场所、延迟、标签和成本；只有快照没有增量序列不能复刻该结论。[DeepLOB](https://arxiv.org/abs/1808.03668)
- Order Flow Imbalance 与短周期价格变化有强联系，但原始研究主要支持价格冲击/同期关系，不能自动解释为可交易预测。[Cont, Kukanov, Stoikov](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1712822)

因此正规的产品路线不是继续堆技术指标，而是：交易所第一方微观结构 + 点时衍生品/跨资产/宏观 + 严格 OOS + 成本与执行模型 + 组合风险 + 可审计发布。

## 过拟合控制

当前强制规则：

1. 时间序列按 walk-forward 切分，训练与验证之间 purge 至少覆盖标签 horizon；必要时 embargo。
2. scaler 只拟合训练段；所有可交易特征至少滞后一周期。
3. 每次试验写入 trial ledger，失败试验也保留，避免只数赢家。
4. 同一 OOS 段不得反复调参后继续称为 OOS；最终应保留从未看过的 lockbox。
5. 新因子以组为单位消融，要求多数折叠稳定改善费用后指标，而不是一次全历史最好。
6. 使用 Deflated Sharpe 修正试验数量；候选足够多时补 PBO/CSC-V。[Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)、[PBO](https://carmamaths.org/resources/jon/backtest2.pdf)
7. 报告分币种、周期、月份、波动 regime 和数据质量状态；平均值不能掩盖崩溃区间。

模型治理参考 2026 年生效的美国联储 [SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm)：风险强度匹配、首次使用前验证、OOS/OOT 测试、持续监控、清单和文档。它是治理参考，不代表本项目受其监管或通过监管认证。

## 官方接口限制带来的数据缺口

| 数据 | 官方可获得性 | 产品结论 |
|---|---|---|
| Binance K 线 | 单次最多 1,500，包含 close time | 可分段回补；必须排除未收盘 K 线 |
| Binance funding history | 单次最多 1,000 | 可分段保存，需核对缺口 |
| Binance OI history | 仅最近约 1 个月 | 三年历史不能从当前官方接口补齐 |
| Binance global/top long-short | 仅最近约 30 天 | 必须从现在持续采集或购买授权数据 |
| Binance taker buy/sell | 仅最近约 30 天 | 同上 |
| FRED 当前序列 | 会修订 | 历史研究必须使用 ALFRED real-time period/vintage dates |

官方目录：[Binance USDⓈ-M Market Data](https://developers.binance.com/en/docs/derivatives/usds-margined-futures/market-data/rest-api)、[FRED 与 ALFRED](https://fred.stlouisfed.org/docs/api/fred/fred_vs_alfred.html)。

## 已执行的验证

| 验证 | 结果 |
|---|---|
| 预测端全量 pytest | 121 passed |
| 交易端全量 pytest | 59 passed |
| 本机 HTTP 影子闭环 | 1 ticket、1 shadow order、1 receipt，PASS |
| 参考 data_service 原测试 | 3 passed |
| 新特征库完整性 | quick_check=ok；raw/enhanced 各 2,587,737 行 |
| 外部 2.2 GB Parquet 真值适配 | SHA 验证通过；最新更新状态降级；24 个字段仅影子 |
| Brain 旧缓存强制重训 | 20 shadow、5 rejected、0 candidate、0 live；已确认为旧短缓存路径，不代表新特征库评估 |
| 旧历史费用后诊断 | -13.48%，不能证明真实交易收益 |

## 进入真钱前的最低证据

- 至少一个从未用于调参的完整 OOS/lockbox 周期费用后为正，且 DSR/PBO、回撤、尾部、容量均通过批准阈值。
- 至少 30 天连续 shadow 和约定周期的 Bybit testnet，票据到成交能一一对账。
- 故障注入覆盖断网、重复消息、丢消息、限流、时钟漂移、部分成交、撤单/成交竞争和进程重启。
- 策略、数据、代码、模型、配置和批准 ID 可重现；任何一个 SHA 不一致都停止晋升。
- 小流量 live 还需独立人工批准，且每日损失、净值回撤、保证金和 kill switch 都能实际阻断新风险。
