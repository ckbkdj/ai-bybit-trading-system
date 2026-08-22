# 模型风险与量化研究标准

更新时间：2026-08-22

## 目标和边界

目标是形成可审计、可复放、可停止、可分阶段发布的量化产品。目标不是承诺收益。任何历史命中率、回测收益或 AI 文本都不能替代实盘成交证据和风险批准。

模型治理采用“概念合理性、持续监控、结果分析/回测、独立质疑和变更控制”的框架，参考美国联储的 [Model Risk Management guidance](https://www.federalreserve.gov/frrs/guidance/supervisory-guidance-on-model-risk-management.htm) 和 [SR 11-7](https://www.federalreserve.gov/boarddocs/srletters/2011/sr1107a1.pdf)。

## 当前模型清单

| 模型 | 角色 | 训练输入 | 输出 | 是否能独立出票 |
|---|---|---|---|---|
| LSTM Keras v2 | 点位/收益弱先验 | 历史 K 线和训练期允许的特征 | 预测价、收益、RMSE、融合方向 | 否 |
| Brain HistGradientBoosting | 正式方向分类与置信度 | historical-kline-only Brain 特征 | long/flat/short 概率、期望收益、发布阶段 | 只有 live 工件且全部质量门禁通过 |
| 在线校准器 | 纠偏/缩放/阈值 | 已到期且结算的预测 | 校准方向、confidence、状态 | 否；状态不为 valid 时禁止出票 |
| 内网 LLM | 结构化上下文辅助 | 脱敏快照 | -1..1 辅助分 | 否 |

## 已修复的验证污染

- LSTM 不再先用全量数据拟合、再把尾部当“验证”。现在先确定 purged chronological holdout，训练 dataset 只包含 holdout 前的数据，purge 至少覆盖完整 LSTM window，验证集从未参与 fit。
- Brain 不再直接 80/20 相邻切分。训练/验证之间 purge 至少等于预测 horizon，并记录准确边界。
- scaler 默认每轮只在训练段重新拟合，防止验证期分布进入缩放参数。
- 生产切分提供 purge/embargo walk-forward；scikit-learn `TimeSeriesSplit(gap=...)` 是参考下限，不代替本系统对 label horizon 和 window 的显式隔离。[官方文档](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)

## 研究试验与过拟合控制

每一次实际 Brain 训练把数据签名、参数哈希、代码版本、状态和指标追加到 `research_trials.sqlite3`。拒绝的试验也计数，不能只保留赢家。旧历史有 15,512 个运行事件，但混合了真实拟合、跳过签名和调度重复，无法恢复独立试验数；报告同时披露新治理试验数和旧运行事件数，并把后者作为保守 DSR 敏感性上界。正式选型还必须在足够多候选时报告 PBO/CSC-V。方法依据：[Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)、[The Probability of Backtest Overfitting](https://carmamaths.org/resources/jon/backtest2.pdf)。

禁止：在同一 OOS 段反复调参、删除失败试验、看完整历史后定义 regime、用最终标签修正历史实体/链上地址、用当前宏观修订值回填旧日期、把多次重叠预测当成独立交易。

## 强制评估输出

预测层：long/short precision 与 recall、balanced accuracy、log loss/Brier、可靠性曲线、ECE、收益/波动/MAE/MFE 误差、分 regime/币种/周期/月份稳定性、OOD 与缺失率。

交易层：费用前后收益、成交率、滑点、资金费率、profit factor、turnover、容量、日/周/月收益、Sharpe/Sortino、收盘到收盘与 intratrade 最大回撤、最长回撤期、尾部损失、连续亏损、压力场景。

组合层：同时持仓、总/净敞口、相关簇暴露、symbol 与 factor contribution、数据源中断、交易所限流/断线、极端 spread 和延迟压力。

## 发布状态机

```text
rejected -> 不可用于推理
shadow   -> 只记录，不出票
candidate -> 只有显式 testnet 策略可出票
live     -> 只有人工证据包晋升后，才可能通过默认出票门禁
```

训练永远最多生成 candidate，不自动生成 live。`promote_brain_model.py` 只接受 candidate，要求不少于 30 天 shadow、测试网完成、费用后 OOS 为正、回撤在批准限额内和 kill-switch 演练均有证据，并记录 approval id、候选哈希和证据哈希。即使模型是 live，交易服务仍保留最终否决权。

## 当前历史证据结论

2026-08-22 对 `online_learning.sqlite3` 的 3,255 条已结算记录进行了成本与组合约束复放：

- 严格 live 门禁：3,255 条全部缺少 recorded direction，合格交易 0；因此不能计算真实策略收益或真实最大回撤。
- 仅供诊断的旧数据方向推断：按预测收益正负猜方向、8% 单笔暴露、24% 总暴露、11 bps 双边手续费和 6 bps 往返滑点，共 304 笔，收益约 -13.48%，收盘到收盘最大回撤约 13.48%，费用后胜率约 13.49%，profit factor 约 0.065。
- 数据没有 intrabar 路径，也不是交易所 fills/receipts，不能复原止损、部分成交、真实滑点和 intratrade 回撤。

所以当前结论是 `profitability_not_demonstrated`，不是“过去能盈利”。完整机器报告位于 `ai_bot3/ai_bot3/model_results/evaluation/strategy_audit.json`（运行时生成）。

同日用本地 OHLCV 缓存按新规则强制重训 25 个 Brain 组合：20 组因只有约 540–600 行而达不到最低训练样本，进入 shadow；其余 5 组非 flat 方向准确率低于 50% 基线，全部 rejected。结果为 **0 candidate、0 live、20 shadow、5 rejected**。机器报告位于 `ai_bot3/ai_bot3/model_results/evaluation/brain_retrain_report.json`。
