# 全量 Bug 与运行证据审计

更新时间：2026-08-23

## 最终结论

本轮不是简单增加指标，而是从入口、数据、训练、推理、出票、执行、恢复、风控、日志和历史证据逐层检查。阻断级工程问题已修复并通过离线/影子回归，但历史数据不能证明策略盈利，真实 Bybit testnet 也尚未验收，因此当前版本只可作为 **shadow 工程候选**。

## 从两年运行日志发现的问题

| 证据 | 规模/次数 | 说明 |
|---|---:|---|
| `main_forecast` | 11,285,589 行，约 0.94 GiB | 预测确实长期运行，但稳定性问题严重 |
| Python traceback | 684,068 | 不能以“进程还活着”代替健康性 |
| resource tracker KeyError | 681,150 | 多进程资源跟踪反复损坏 |
| 进程重启 / 强杀 | 274,892 / 139,145 | 原运行方式存在重启风暴和误杀风险 |
| TensorFlow 重初始化 | 617,580 | 模型后端重复初始化，浪费资源并放大故障 |
| CUDA 不可用日志 | 238,411 | GPU 配置与实际环境不一致 |
| `main_train` DataFrame 碎片警告 | 205,226 | 特征逐列插入导致性能退化 |
| 训练跳过 | 203,041 | 调度事件多不等于有效训练次数 |
| `liqmap` 成功 / 超时 / fallback | 151,975 / 4,420 / 2,725 | 数据链可用但必须标记降级，不能把 fallback 当真实源 |

完整机器报告：`ai_bot3/ai_bot3/model_results/evaluation/runtime_log_audit.json`。

## 已修复的主要缺陷

| 等级 | 原问题 | 当前处理 |
|---|---|---|
| P0 | 旧配置中曾出现明文 LLM key | 当前工作树已清空并改为环境注入；旧 key 必须轮换，历史 Git 清理需另行授权 |
| P0 | live 只靠模式/单开关可能误启 | 默认 shadow；live 同时要求 `BYBIT_ENABLE_LIVE=true`、`BYBIT_LIVE_APPROVAL_ID`、凭证 |
| P0 | 退出动作可能被 kill switch/健康门禁一起拦住 | 风险降低动作继续允许；门禁只阻止增加风险 |
| P0 | REST 成功容易被误当成交 | 状态分离为受理、确认、部分成交、成交；以私有流 `execId` 去重 |
| P0 | 未知提交结果可能重复下单 | 确定性 `orderLinkId`，实时/历史订单及成交恢复；查询失败时关闭而不重发 |
| P0 | 私有流会把人工/旧 bot 订单混入新账本 | 只处理当前执行库已登记订单；已知订单更新失败触发 kill switch |
| P0 | 加仓后曾临时移除保护，且计数写可变 INI | 加仓策略默认禁用；旧规则只保留为待验证假设 |
| P1 | 训练/推理特征存在同周期泄漏 | 训练与服务均显式 shift；holdout 前 purge，验证段不参与 scaler/fit |
| P1 | 未完成 Binance K 线可能进入特征 | 只接受 close time 已结束的 K 线 |
| P1 | 训练特征缺失时推理补 0 | 改为严格特征顺序合同；缺列或版本不符直接拒绝 |
| P1 | 多周期训练与在线推理 PIT 合并不一致 | 多周期功能保持禁用，直至同一 as-of merge 验证完成 |
| P1 | 在线校准曾使用错误代理值 | 改用模型 `predicted_return`；新结算会记录方向、命中、成本和时间 |
| P1 | K 线特征 SQLite 索引/页面损坏 | 训练失败关闭；完成非破坏重建，不覆盖源库，不自动切生产路径 |
| P1 | `with sqlite3.connect()` 被误认为会关闭连接 | 所有相关路径改为显式 context manager/closing，解决 Windows 文件锁与句柄泄漏 |
| P1 | 旧进程清理可能影响无关 Python | 仅清理当前服务的后代进程 |
| P1 | TP 安装、最长持仓退出存在竞争 | 时间退出开始后不再新建 TP；退出使用确定性只减仓订单 |
| P2 | 活跃交易服务依赖大型旧 `bybit.py` | 新建最小 `exchange_gateway.py`；旧模块保留供历史版本回看 |
| P2 | CORS 通配符且带凭证 | 默认限制 localhost；通配符时自动关闭 credentials |
| P2 | FastAPI 生命周期接口版本不兼容 | 改用当前和旧运行均支持的 router lifecycle 注册 |

## 特征库重建证据

- 损坏源库：`data/kline_feature_store.sqlite3`，约 2.81 GB；只读审计发现 malformed/index 错误。
- 新候选库：`data/kline_feature_store.rebuilt.20260822.sqlite3`，约 2.76 GB。
- 原始 K 线：2,587,737 行、25 个 symbol/timeframe/source 组。
- 增强特征：2,587,737 行、25 组，与原始行数一一对应。
- SQLite `quick_check=ok`，复制失败 0，生产路径未改变。
- 机器报告：`kline_feature_store_rebuild_report.json` 与 `rebuilt_feature_store_audit.json`。

## 历史收益与回撤能证明什么

`online_learning.sqlite3` 有 685,984 条预测，但只有 3,255 条 settled；历史 settled 行缺 recorded direction、confidence、model version、成本和真实成交回执，不能还原真实交易。

- 严格按现在 live 门禁复放：3,255 条全部因缺方向不合格，0 笔交易；不能计算真实策略回撤。
- 仅供诊断地用预测收益正负猜方向，并限制 8% 单笔、24% 总敞口、11 bps 往返手续费、6 bps 滑点：304 笔，约 -13.48%，收盘到收盘最大回撤约 13.48%，费用后胜率约 13.49%，profit factor 约 0.065。
- 缺 intrabar、订单、成交、手续费和 funding 路径，无法复原止损、部分成交、盘中回撤或实际 PnL。
- 在 15,512 个旧训练运行事件的保守多重试验上界下，诊断结果的 Deflated Sharpe 概率约 `4.69e-05`。

结论只能是 `profitability_not_demonstrated`。不能保证过去盈利，更不能据此保证未来盈利。

## 尚未关闭的上线阻断项

1. 使用真实测试网凭证完成部分成交、撤单竞争、重启恢复、TP/SL、限流、断网、DCP 和 position mode 验收。
2. 用新字段连续积累足够长的 shadow 预测、票据、订单和成交回执；现有旧数据库不能替代。
3. 完成费用、滑点、funding、容量、intratrade drawdown 和压力场景回测。
4. 新因子必须走 PIT/OOS 消融和多重试验校正，不能看完全历史再挑规则。
5. `trad_data_service` 最新更新任务需修复可移植路径；资产分类错标需独立治理。
6. 新特征库必须先经用户确认、备份和 shadow 对比后再切路径。

