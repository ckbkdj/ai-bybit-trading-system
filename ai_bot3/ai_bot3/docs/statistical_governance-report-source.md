# DSR 与 CSCV/PBO 研究门禁依据

受众：`Profitability-First Alpha Rebuild` 的研究、审核与发布人员

日期：2026-08-24

范围：费用后 development/lockbox 的多重试验与回测过拟合门禁，不讨论未来收益保证。

## 直接结论

候选发布必须同时满足 `DSR probability >= 0.95` 与 `CSCV PBO <= 0.05`。DSR 使用同步的组合逐日 mark-to-market 收益，并把本轮候选配置、因子消融两臂、适用 horizon 以及历史流水线试验计入预注册试验总数。PBO 使用 development outer OOS 上所有预注册候选配置的同步收益矩阵；它只决定通过或拒绝，不能用于在 outer OOS 上重新选参数。

最终 lockbox 不运行备选配置。它只对 development 已冻结的最终路径重算 DSR，并继承 development 的 PBO。这样不会为了统计报告额外窥视或消费 lockbox。

## 原始依据

Bailey 与 López de Prado 的 DSR 论文指出，PSR 会使用样本长度和收益前四阶矩，并把多重试验产生的选择偏差加入拒绝基准；论文示例以 95% 置信水平判断发现是否成立。因此本项目把 0.95 预注册为不可下调的最低 DSR 概率，而不是等看到结果后再选择阈值。[The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting and Non-Normality — David H. Bailey and Marcos López de Prado, 2014](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)

Bailey、Borwein、López de Prado 与 Zhu 的 PBO 论文要求先构造 `T × N` 同步策略收益矩阵，将行切成偶数个等长连续子样本，枚举一半为 IS、另一半为 OOS，在 IS 选最优配置后检查其 OOS 相对排名。PBO 是该配置 OOS 低于中位数的频率；论文给出的惯常拒绝线是 PBO 大于 0.05。因此本项目固定 8 段 CSCV、使用费用后组合逐日 MTM 收益，并要求 PBO 不高于 0.05。[The Probability of Backtest Overfitting — David H. Bailey, Jonathan M. Borwein, Marcos López de Prado and Qiji Jim Zhu, revised 2015](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf)

## 实现假设与限制

- CSCV 行是共同 UTC 日历上的组合收益；无交易日保留为零，不能靠删除空闲期抬高结果。
- 各列必须来自相同 outer OOS 日期、相同成本模型和相同风险约束。数据不同步、少于两个策略、样本不足、非有限值或 IS 最优策略并列时失败关闭。
- DSR 的 Sharpe 不年化，所有矩、样本数和试验 Sharpe 保持相同日频单位；策略间 Sharpe 标准差至少取样本误差下限，避免少量候选低估选择偏差。
- 当前试验总数采用保守上界，未按策略相关性折算“有效独立试验数”。这可能增加假阴性，但不会放宽 Candidate。
- DSR 与 PBO 只能说明历史证据在既定统计门槛下较难由选择偏差解释，不能保证未来盈利或保本。

## Claim-to-source ledger

| 结论 | 来源 | 日期 | 访问说明 |
|---|---|---:|---|
| DSR 同时校正非正态、样本长度与多重试验选择偏差 | Bailey & López de Prado, *The Deflated Sharpe Ratio* | 2014-07-31 | 作者公开 PDF，2026-08-24 查阅 |
| 95% 可作为预先确定的发现置信门槛 | 同上 | 2014-07-31 | 论文示例明确按 95% 判断 |
| CSCV 使用同步 `T × N` 矩阵、偶数等长分区和对称 IS/OOS 组合 | Bailey et al., *The Probability of Backtest Overfitting* | 2015-02-27 | 作者公开 PDF，2026-08-24 查阅 |
| PBO 衡量 IS 最优配置 OOS 低于中位数的频率，惯常拒绝线为 0.05 | 同上 | 2015-02-27 | 论文第 3.1 节 |
