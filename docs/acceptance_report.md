# 验收矩阵

更新时间：2026-08-22

状态含义：PASS 为离线或本机影子自动测试已通过；GATED 为代码已失败关闭，但仍需使用用户测试网凭证做外部验收。

| # | 验收项 | 状态 | 证据 |
|---:|---|---|---|
| 1 | ForecastEnvelope 同时通过 Pydantic/JSON Schema | PASS | `test_contracts_v1` |
| 2 | Legacy adapter 固定样本固定输出 | PASS | golden forecast id 测试 |
| 3 | 重复 ticket_id 不产生第二订单 | PASS | `test_duplicate_ticket_never_creates_second_order` |
| 4 | 过期票据拒绝 | PASS | `test_expired_bad_quality_and_unconfirmed_testnet_are_fail_closed` |
| 5 | 缺少风险字段拒绝 | PASS | `test_hold_and_missing_risk_fields_are_rejected` |
| 6 | 数据质量低于要求不下单 | PASS | 契约与风险测试 |
| 7 | 实时价格偏离拒绝 | PASS | `test_position_version_price_and_kill_switch_are_rejected` |
| 8 | 数量/价格符合 step/tick | PASS | `test_precision_is_normalized_down_without_exceeding_limits` |
| 9 | REST 成功、未收到成交不能标 FILLED | PASS | `test_rest_success_does_not_mark_filled...` |
| 10 | 部分成交累计 | PASS | 同上 |
| 11 | 重复 Filled/execId 不重复记账 | PASS | 同上 |
| 12 | 重启后按 orderLinkId 恢复且不重发 | PASS | `test_restart_recovery_uses_order_link_id_without_resubmit` |
| 13 | 新票替代旧票后旧票不能执行 | PASS | `test_superseded_ticket_cannot_continue` |
| 14 | 仓位版本冲突拒绝 | PASS | position conflict 测试 |
| 15 | 回测不能读取未来 available_at | PASS | `test_available_at_blocks_future_leakage` |
| 16 | 宏观修订不能倒灌历史 | PASS | `test_macro_revision_cannot_backfill_old_snapshot` |
| 17 | 数据源中断降级/禁新风险 | PASS | PIT outage 与 risk outage 测试 |
| 18 | 测试不能触发真实订单 | PASS | ShadowExchange/no-private-network 测试及 E2E |
| 19 | 日志不打印 key/secret/signature | PASS | `test_logger_redacts_credentials_and_signatures` |
| 20 | 交易端不导入预测内部模块 | PASS | `test_execution_code_does_not_import_prediction_internals` |

额外已通过：claim lease、游标 outbox、不可变冲突、费用后门槛、REDUCE/CLOSE、CANCEL、cancel/fill 竞争、止损附带、幂等止盈子单、限流头、kill switch、研究 checkpoint/revision/Tier A blackout、PIT vintage、因子语义、walk-forward/purge/embargo、成本回测和因子组消融。

本机 HTTP 影子 E2E 结果要求并已观测：`cursor=1`、`state=SUBMITTED`、`shadow_order_count=1`、`control_plane_receipt_count=1`。

最终全量回归：预测端 `pytest` 为 **81 passed / 6 skipped / 0 failed**（跳过项均为原项目可选模型依赖路径）；交易端 `unittest` 为 **25 passed / 0 failed**。两个项目的 Python 文件均通过语法编译。

## 外部测试网门禁

以下不能用假客户端证明，状态为 GATED：真实 Bybit testnet 的账户 position mode、最小数量/价格、私有 WebSocket 重连、实际限流响应头、附带止损/止盈子单、交易所维护时段和网络分区。代码默认禁止在这些健康条件未确认时增加风险；完成测试网证据前不得进入主网。

官方复核依据：

- 下单异步确认、orderLinkId、TP/SL、reduceOnly：<https://bybit-exchange.github.io/docs/v5/order/create-order>
- 私有 execution/execId：<https://bybit-exchange.github.io/docs/v5/websocket/private/execution>
- UID/endpoint 限流响应头：<https://bybit-exchange.github.io/docs/v5/rate-limit>
- 仓位 TP/SL 数量自动调整：<https://bybit-exchange.github.io/docs/v5/position/trading-stop>
