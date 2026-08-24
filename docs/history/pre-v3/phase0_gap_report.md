# 阶段 0 差异与准入报告

> 2026-08-22 更新：本报告记录的是改造前基线。安全基线、版本化契约、票据 outbox、执行状态机、幂等、持久化风控、私有流、重启对账、PIT、慢研究和本机 HTTP 影子闭环现已实现。最终结构与验收见 `architecture_v1.md`、`acceptance_report.md` 和 `implementation_manifest.md`。

审计日期：2026-08-21  
审计状态：完成静态主链路审计；未运行交易代码、未发起任何订单。

## 总体判断

目标架构方向成立，但当前代码与原任务书之间存在重大差异。当前状态对“继续只读设计”是 GO，对“直接修改并启用真实交易”是 NO-GO。

## 目标假设与实际代码

| 原目标/假设 | 实际代码 | 影响 |
|---|---|---|
| 预测与交易已有可扩展接口 | 实际为最新 JSON + `/results`，交易只读 `trend` | 必须先建 legacy adapter 和 schema |
| 交易端可消费任意币种预测 | 读取函数硬编码 `XRPUSDT/scalping` | 非 XRP 币种通常拿不到预测 |
| 数据质量字段可用于交易门控 | 可靠性检查只在 `/predict` 完整体现；交易用 `/results` | 陈旧/异常预测仍可能影响决策 |
| 预测端给出操作意图 | 预测 JSON混有 `trade_actionable` 与 `target_leverage`，但无操作票 | 语义不稳定，不能直接执行 |
| 交易端有明确订单生命周期 | 只有 REST 调用、open orders/positions 轮询 | 无幂等、部分成交和恢复状态机 |
| 有本地执行数据库 | 只有 INI 状态和账户总值快照 | 无订单、成交、手续费和回执账本 |
| 可安全运行自动测试 | import 主脚本会初始化主网客户端 | 必须先解除导入副作用并提供 fake exchange |
| 旧版本只是只读历史 | 多个历史版本仍含明文凭证和 webhook | 所有凭证必须统一撤销与清理 |
| 可直接按阶段实施 | 两个项目都没有 Git 基线 | 必须先建立可回滚代码基线和数据备份 |

## P0 阻断项

1. **撤销凭证**：所有源码中出现过的 Bybit key、secret 和 webhook 都按泄露处理，立即撤销重建。
2. **禁止默认主网**：增加默认 shadow/testnet；没有显式生产授权时，任何测试和启动都不能连接主网。
3. **可信预测通道**：停止 `verify=False`；加入 TLS 校验、认证/签名、schema version、过期与重放检查。
4. **可回滚基线**：代码建立 Git 仓库或等价不可变快照；SQLite、INI、模型和结果目录另行备份。
5. **安全测试边界**：交易模块不得在 import 时创建客户端；测试强制注入 fake exchange，并断言没有真实网络下单。

## P1 必须在真实切换前完成

- OperationTicket 唯一 ID 和交易端幂等表。
- 账户/仓位版本、组合杠杆、日亏损和 kill switch。
- 数量/价格精度规范化与费用后收益门槛。
- REST 提交与私有 WebSocket 订单/成交确认。
- 部分成交、重复成交、cancel/fill 竞争和重启 reconcile。
- 新开仓同时建立保护，或在保护失败时执行可证明的补偿动作。
- 每笔 fill 生成 ExecutionReceipt 并回传预测端。

## 当前测试面

`ai_bot3` 已有反泄漏、行情上下文、在线校准、情绪、训练元数据等测试。交易端只有极小的手工测试文件，没有可证明安全的自动订单测试。

本次没有运行现有测试，原因是阶段 0 不修改业务，而且交易主模块存在 import 即连接主网的副作用。必须先完成安全测试隔离，才能运行交易端测试发现。

## 推荐下一阶段

不要马上实现完整 ForecastEnvelope 或状态机。先做“阶段 0.5 安全基线”：

1. 用户撤销并重建外部凭证。
2. 代码与数据分别备份，建立 Git 忽略规则和初始提交。
3. 把凭证改为环境变量，并从所有历史脚本移除。
4. 把 Bybit 客户端改为依赖注入，默认 fake/shadow/testnet。
5. 建立一条 no-real-order 测试。

完成后再进入阶段 1：只新增三份契约、JSON Schema、Pydantic 模型、legacy adapter 和 golden tests；不改变现有实盘执行结果。

## 本阶段产物

- `docs/current_prediction_contract_audit.md`
- `docs/current_trading_contract_audit.md`
- `docs/current_order_flow.md`
- `docs/current_database_flow.md`
- `docs/phase0_gap_report.md`
