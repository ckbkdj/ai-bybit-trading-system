# 旧版经验复用与真实数据来源

更新时间：2026-08-23

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
| 账户、订单、成交 | Bybit V5 wallet/order/execution/transaction log | 最终风险和真实执行 | 必须用私有流和恢复查询；REST 接受不是成交 |

## `trad_data_service` 的吸收方式

这套参考服务的价值主要在产品化数据流程，而不是“把 5,332 个字段全喂给模型”：

```text
raw -> factor -> candidate -> release gate -> full audit -> atomic promotion
```

已吸收的设计包括：canonical/candidate 分离、只有 promotion 可改正式数据、失败保持旧 SHA、来源登记、依赖闭包、隔离清单、运行收据和原子发布。当前面板为 466,537 行、5,332 列、486 个标的，日期 2020-04-16 至 2026-08-20；最后成功运行产生 SHA `444ef96c…`。

同时发现两个不能忽略的问题：

1. 最新更新任务为 BLOCKED，原因指向旧绝对路径下缺失 lifecycle evidence；最后成功 canonical 未被破坏，但更新链不完全可移植。
2. `asset_family` 历史标签出现明显错标/漂移，例如“比特币现货”历史段包含 AMZN、BIL、FXY、IBB；SPY、QQQ、USO、GBTC 等标签也发生过变化。

因此新适配器只读取 `symbol/ts/close/asset_family` 四列，以 SPY、QQQ、TLT、GLD、USO、UUP、GBTC、COIN 明确白名单选取，标签只作为审计输出而不作为选择条件。真实 SHA 已验证，当前能生成 1/5/20 个交易日共 24 个观察字段，但全部标记为 `shadow_only_pending_pit_oos_ablation`。

## 下一批数据优先级

1. Bybit 第一方盘口增量和逐笔成交：用于 spread、OFI、CVD、可成交性和滑点模型。
2. Bybit/交易回执：建立真实 fills、fee、funding、MFE/MAE 和盘中回撤闭环。
3. 从现在开始持续保存 Binance/Bybit OI、资金、多空和 taker；官方接口无法补齐全部三年历史。
4. FRED/ALFRED vintage 宏观数据：必须保存当时可见版本，不能用今天修订值回填过去。
5. 经授权的链上供应商或自建节点：地址标签也要版本化。

