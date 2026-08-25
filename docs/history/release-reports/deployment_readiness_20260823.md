# 部署就绪度与证据结论

日期：2026-08-23
唯一准确状态表述：**可部署的 shadow 工程候选，持续运行能力仍需长时间 soak 验证。**

## 评分

| 维度 | 当前评分 | 证据解释 |
|---|---:|---|
| 架构与边界 | 8.5/10 | 多周期组合决策、原子 release、不可变 outbox、fencing、ownership、incident/reconcile 已落地 |
| 离线与本机 shadow 工程 | 8/10 | 121 个预测测试、59 个交易测试、跨进程 HTTP 影子闭环通过 |
| 数据治理 | 7/10 | 258 万行特征语义审计通过；PIT/hash/attrition 已有；外部 provider 和依赖证明仍不完整 |
| 真实交易所证据 | 3/10 | Bybit 接口边界和假客户端证据具备，但没有本次专用 testnet 账户与真实回执 |
| 模型/盈利证据 | 1/10 | 旧记录不可还原真实交易，诊断结果为负；没有合格 lockbox/candidate/live bundle |
| 主网上线 | 0/10 | `NO-GO`，未授权、未连接、未完成必要门禁 |

## 本轮已证明

| 证据 | 结果 |
|---|---|
| 预测端回归 | 121 passed / 0 failed |
| 交易端回归 | 59 passed / 0 failed |
| 跨进程 shadow E2E | 1 ticket、1 shadow order、1 execution receipt，PASS |
| 参考 `D:\lh\trad_data_service_20260821\data_service` | 3 passed；只读目录仅有 pytest cache 警告 |
| 候选特征库 | raw/enhanced 各 2,587,737；25 组；重复、OHLC、JSON、hash、grid gap 均为 0；25/25 重算 PASS |
| 数据切分 | 五个 symbol × 五个 mode 全部有独立 train/validation/test 和两处 purge gap |
| DB 演进 | control-plane v2、execution v5；记录 migration/checksum/commit；更高 schema 拒绝启动 |
| 执行幂等 | 确定性订单/命令 ID；服务实例租约；epoch takeover；旧 worker 写入被拒绝 |
| 安全扫描 | 当前已跟踪文件未发现字面凭证；`.env.local` 未跟踪 |

## 仍未证明或被门禁阻断

| 项目 | 状态 | 为什么不能跳过 |
|---|---|---|
| 30 天持续 soak | BLOCKED | 代码和 SLO 已有，但尚不存在 30 天连续指标 |
| 真实 Bybit testnet | GATED | 本轮没有专用 testnet 子账户/key，不能伪造部分成交、断线、限流和交易所状态证据 |
| 供应链 | BLOCKED | Python 依赖未完全锁定，缺当天 vulnerability attestation |
| candidate/live release | BLOCKED | 没有同时绑定模型、数据、成本、代码、证据、批准的已晋升 bundle |
| 费用后 OOS/lockbox 盈利 | BLOCKED | 尚无从未参与调参的完整正向证据 |
| 成本模型 | PROVISIONAL | 接口与分段校准已实现，但缺足量真实 receipt，低样本只走保守 fallback |
| 主网 | NO-GO | 以上任一项未完成都不能授权真钱 |

## 历史盈利与回撤

`online_learning.sqlite3` 有 685,984 条预测，但仅 3,255 条 settled，而且旧行缺方向、置信度、模型版本、真实订单/fill、费用和 funding。按现行严格门禁复放为 0 笔合格交易，无法计算真实策略回撤。

仅用于发现风险的推断诊断：304 笔、8% 单笔敞口、24% 总敞口、11 bps 手续费、6 bps 滑点时，累计约 **-13.48%**，收盘到收盘最大回撤约 **13.48%**，胜率约 **13.49%**，profit factor 约 **0.065**。缺 intrabar 和真实 fill，这不是实盘回测。

因此结论是 `profitability_not_demonstrated`。不能保证过去记录盈利，也不能保证未来盈利。

## 下一次可改变结论的证据

1. 在专用 Bybit testnet 子账户完成 OPEN/INCREASE/REDUCE/CLOSE/CANCEL/REPLACE、部分成交、cancel/fill race、断线重连、重启接管、TP/SL、限流和 position mode 验收。
2. 运行至少 30 天、每分钟采样、连续性和增长斜率全部达标的 shadow soak。
3. 在冻结 lockbox 上以真实 fee/slippage/funding 模型得到费用后结果，并报告 DSR/PBO、尾部回撤、容量和分 regime 稳定性。
4. 生成并人工批准完整 StrategyReleaseBundle；预测端和执行端 release id、artifact SHA、code commit 完全一致。
5. 锁定可重建依赖，附带漏洞扫描/SBOM 证明，再讨论小流量主网窗口。
