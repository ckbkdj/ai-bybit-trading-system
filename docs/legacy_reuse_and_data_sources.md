# 旧版经验复用与真实数据来源

更新时间：2026-08-24

## 旧版没有被扔掉

旧 v4.1 完整实现以 Git 版本 `3f36b65` 作为来源锚点。两年线上经验被拆成“可直接保留的安全不变量、需要重新验证的交易假设、必须淘汰的危险实现”三类，并固化到 `BybitContractBotV4/legacy_experience.py` 和回归测试中。

| 旧经验 | 精确恢复结果 | 新系统处置 |
|---|---|---|
| GTC 等待两根 K 线减 12 秒 | 3m=348 秒、5m=588 秒、15m=1788 秒 | 保留为 entry-timeout 候选，需按成交率/机会成本复放 |
| 初次分配账户 0.6% | `initial_equity_fraction=0.006` | 思路保留；执行改成止损风险、名义、保证金和组合容量取最小值 |
| 最多 3 次入场、最多 2 次盈利加仓 | 精确保存 | 默认关闭；因为旧实现曾移除保护并用 INI 计数 |
| 40/50/60/80/100/120% 杠杆收益后的锁盈 | 精确公式和杠杆换算已测试 | 保留为 shadow 策略模板，不与新 TP/SL 同时无条件启用 |
| 止损只能越来越紧 | 多单取更高、空单取更低 | 作为正式安全不变量保留 |
| 波动带与入场次数决定分档退出 | 32；或 41/58；或 41/58/80/100 阶梯 | 保留为待消融退出模板；剩余仓位语义必须显式 |
| BTC 锚点和趋势过滤 | 避免山寨逆大盘 | 转为跨资产/regime 候选，不能写死相关方向 |
| 75x/100x 和 31% 目标 | 放大微小预测 | 仅保留历史标签解释；实盘杠杆由风控上限控制，默认远低于旧值 |

被明确淘汰的不是经验，而是不安全的实现方式：直接读取预测内部 JSON、共享可变 INI、REST 返回即算成功、加仓时撤保护、全局杀 Python 进程、订单状态不可恢复。

## 当前真实使用的数据

| 因子族 | 从哪里抓取/读取 | 实际用途 | 主要限制 |
|---|---|---|---|
| OHLCV | Binance USDⓈ-M Futures Kline API，经 CCXT/本地缓存 | LSTM、Brain、技术量价 | 执行在 Bybit，需测试 basis；未完成 K 线已排除 |
| funding | 本地 Coinglass wrapper 优先；缺失时 Binance funding 回退 | 衍生品拥挤度 | 历史 API 可回溯窗口有限 |
| OI、多空、主动买卖 | 本地 Coinglass 文件；部分 Binance Futures API | 结构融合/研究 | Binance OI 仅约 1 个月，多空与 taker 仅约 30 天，不能声称三年完整历史 |
| 爆仓图 | `data/{BASE}.json` 与 `coinglass_metrics` | 爆仓压力、距离、热度 | 是结构估计，不是必然支撑/阻力；要求新鲜时间戳 |
| 新闻/日历/鲸鱼/恐慌贪婪 | 本地 JSON wrapper | 低权重上下文 | 来源未登记为 Tier A 时不能独立触发交易或解除封锁 |
| 内网 LLM | 内网 OpenAI-compatible Qwen 服务 | 结构化辅助分和摘要 | 不训练、不下单、失败返回中性；key 环境注入 |
| 在线校准 | `online_learning.sqlite3` 已到期预测 | bias、scale、阈值 | 旧数据字段不全；状态不 valid 时禁止出票 |
| 跨资产日频 | `D:\lh\trad_data_service_20260821\data_service` canonical Parquet | 当前只做 shadow/慢研究 | 显式白名单、30 小时滞后、SHA 收据校验，不直接融合 |
| Bybit 盘口/逐笔 | Bybit 官方历史归档 + V5 实时 public orderbook/trades | spread、depth、microprice、OFI/CVD、扫单滑点研究 | 正式 37 天五币种覆盖和 OOS 消融仍在建立；盘口不能替代真实 fill 回执 |
| Bybit funding/OI/basis | Bybit V5 官方历史 REST | 实际结算 funding、5 分钟 OI 变化、1 分钟 mark/index basis | 历史回放保留响应哈希；liquidation 不是该 REST 的组成部分 |
| Bybit liquidation | `allLiquidation.{symbol}` 实时流和保留的逐条原始事件 | 5 分钟多/空强平名义不平衡 | v1 方向解释已失效并重建为 v2；官方没有对应历史 REST，当前覆盖不足 |
| 账户、订单、成交 | Bybit V5 wallet/order/execution/transaction log | 最终风险和真实执行 | 必须用私有流和恢复查询；REST 接受不是成交 |

## `trad_data_service` 的吸收方式

这套参考服务的价值主要在产品化数据流程，而不是“把 5,332 个字段全喂给模型”：

```text
raw -> factor -> candidate -> release gate -> full audit -> atomic promotion
```

已吸收的设计包括：canonical/candidate 分离、只有 promotion 可改正式数据、失败保持旧 SHA、来源登记、依赖闭包、隔离清单、运行收据和原子发布。当前面板为 466,537 行、5,332 列、486 个标的，日期 2020-04-16 至 2026-08-20；最后成功运行产生 SHA `444ef96c…`。

同时发现三个不能忽略的问题：

1. 最新更新任务为 BLOCKED，原因指向旧绝对路径下缺失 lifecycle evidence；最后成功 canonical 未被破坏，但更新链不完全可移植。
2. `asset_family` 历史标签出现明显错标/漂移，例如“比特币现货”历史段包含 AMZN、BIL、FXY、IBB；SPY、QQQ、USO、GBTC 等标签也发生过变化。
3. 面板中的 `x_mcp_fred_*`、ETF flow 和 `x_rot_vix_proxy` 不能仅凭列名复用：同日全局一致性审计没有证明部分 `x_mcp_fred_*` 是单一宏观真值，`x_rot_vix_proxy` 不是 VIX，旧 stablecoin 重建历史按 fetch-time 可用，不能倒填成历史 vintage。

因此盈利重建适配器只读取 `symbol/ts/close` 三列，以 SPY、QQQ、TLT、UUP、GLD、USO、XLV、IBB、FXI、KWEB、COIN、MSTR 明确白名单选取，不根据 `asset_family` 或字段前缀批量纳入。真实 SHA 已验证；这些日收益只有完成相同 outer OOS fold、成本和事件回测下的逐组消融后才可能进入正式特征集。

## 下一批数据优先级

1. 完成 Bybit 第一方盘口、逐笔、funding/OI/basis 的 37 天五币种冻结覆盖并做实际 OOS 消融。
2. 持续采集修正后的 liquidation v2，并用 Bybit shadow/testnet 回执建立真实 fills、fee、partial fill、MFE/MAE 和盘中回撤闭环。
3. 接入 Cboe 官方 VIX 历史和具有可证明 `available_at` 的真实利率数据；来源快照必须带哈希，不能使用名字相似的代理列。
4. FRED/ALFRED vintage 宏观数据必须保存当时可见版本；API key 由部署方环境注入，不能用今天修订值回填过去。
5. ETF/stablecoin flow 只接受可审计发布日期/区块确认时间；旧服务的 current snapshot 或 fetch-time 重建历史只能从首次本地采集后使用。
