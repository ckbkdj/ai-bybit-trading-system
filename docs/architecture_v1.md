# 预测—交易系统架构 v1

更新时间：2026-08-22

## 结论

系统已经从“交易脚本读取预测内部文件并自行解释”改成两个独立服务：

```text
内部/外部数据源
  -> Point-in-Time 特征与数据质量
  -> 快预测 / 慢研究
  -> ForecastEnvelope.v1
  -> 成本与操作票策略
  -> OperationTicket.v1 + SQLite WAL Outbox
  -> HTTP 游标拉取 + claim lease
  -> 本地实时风控与精度换算
  -> Bybit REST 异步受理
  -> 私有 Order/Execution WebSocket
  -> ExecutionReceipt.v1 Outbox
  -> 预测控制面回传
```

`ai_bot3` 不读取账户余额、不计算最终张数、不调用私有下单接口。`BybitContractBotV4` 除契约和 HTTP 客户端外，不导入预测端内部模块，也不读取预测 SQLite、模型结果或阈值。

## 不可变契约

三份契约同时提供严格 Pydantic 模型和静态 JSON Schema。未知字段拒绝，时间统一为带时区 UTC，概率、分位数、成本恒等式和风险字段均有交叉校验。

- `ForecastEnvelope v1`：分布、周期、预测截止点、状态、质量、因子、证据和血缘。
- `OperationTicket v1`：唯一执行边界；只允许 `OPEN/INCREASE/REDUCE/CLOSE/CANCEL/REPLACE`，不发送 HOLD。
- `ExecutionReceipt v1`：每次执行状态修订、订单、成交、手续费和仓位版本的不可变回报。

预测可以增加 revision。票据不能原地修改；替代通过新 `ticket_id` 和 `supersedes_ticket_id`。网络采用至少一次交付，执行采用确定性 `orderLinkId`、本地预留和幂等账本。

## 控制面与状态存储

预测控制面使用独立 SQLite WAL：

- forecasts / operation_tickets 均不可变；
- ticket_delivery_outbox 使用自增整数游标，避免同毫秒丢票；
- ticket_claims 有租约；
- ticket_events 和 execution_receipts 追加写；
- 同 ID、不同内容触发冲突，不静默覆盖。

交易执行库保存 tickets、ticket_events、execution_orders、execution_fills、position_snapshots、risk_runtime、receipt_outbox 和 consumer_cursors。`exec_id` 是成交去重主键，`order_link_id` 是订单幂等主键。

## 执行状态机

主路径：

```text
RECEIVED -> VALIDATED -> CLAIMED -> RISK_APPROVED
         -> SUBMITTING -> SUBMITTED -> ACKNOWLEDGED
         -> PARTIALLY_FILLED -> FILLED
```

终止/否决状态：`REJECTED/EXPIRED/CANCELLED/FAILED/SUPERSEDED/RISK_BLOCKED`。状态只能单调前进；真实成交可以在 cancel/fill 或 supersede/fill 竞争中胜出。晚到的 `New` 不会把 `FILLED` 倒退。

REST 成功只表示异步受理，不表示成交。执行端通过私有订单流确认受理、通过 execution 流的 `execId` 累计部分成交；程序重启按 `orderLinkId` 查询订单和成交，未知提交结果不会盲目重发。

## 风控与保护

交易端有最终否决权。票据时效、数据质量/年龄/源中断、事件封锁、实时价差和价格偏离、市场状态、账户权益、日亏损、保证金使用率、连续亏损冷静期、总杠杆、相关方向暴露、仓位版本、时钟漂移、私有 WebSocket 和 kill switch 都在下单前检查。

最终数量取以下约束的最小值并向下对齐数量步长：止损风险数量、票据名义上限、目标暴露、可用保证金和组合容量。价格按方向对齐 tick size。REDUCE/CLOSE 只从当前实际仓位计算，所有退出订单强制 `reduceOnly`。

风险增加票据的入场单原子附带 Bybit 全仓位止损。入场完全成交后生成确定性、只减仓的止盈子单；重复回调不会重复创建。止盈安装失败时保留已附带止损并打开本地 kill switch，等待人工处置。

Bybit 官方说明：下单响应是异步受理，需要 WebSocket 确认；`orderLinkId` 最长 36 字符且需唯一；`reduceOnly=true` 不能同时设置 TP/SL。实现遵循这些约束：<https://bybit-exchange.github.io/docs/v5/order/create-order>。

## Point-in-Time 与多因子

每条观测区分 `event_time/published_at/available_at/ingested_at/revision_id`。历史快照只选择 `available_at <= simulated_time` 的版本，宏观修订不能倒灌。源中断生成质量事件，并使相关快照进入 blocked/degraded。

正式因子注册表覆盖价格、微观结构、衍生品、链上、美元流动性、增长通胀、风险偏好、黄金、原油、美股、中国、医疗、新闻、时段和质量。上层状态图包含八个分数。每个输入显式标为直接观测、直接资金流或推断轮动代理，并按可靠度、时效半衰期和外部训练的状态权重聚合；没有写死“黄金上涨则 BTC 下跌”。

内网或外部 JSON 数据源通过注入 transport、parser、端点、CA 和运行时 header 接入。密钥不写进 provider 对象的日志或源码。具体源的 URL/凭证属于部署配置，不属于契约。

## 快慢双通道

快通道保存预测并按校准、数据质量、OOD、方向概率和费用后安全边际决定是否出票。慢研究任务具有持久化状态、checkpoint、revision、来源分级、实体映射、情景归一化和结构化 EventImpactVector。仅经验证的 Tier A 来源可触发或解除事件封锁；Tier C 单独出现不能触发强交易动作。

## 评估协议

评估使用 walk-forward、purge 和 embargo；PIT 数据截止点强制执行。回测计入 maker/taker fee、资金费率、滑点和部分成交，同时输出预测质量与交易表现。新增因子组通过同一组样本外折叠做消融，只有平均改善和改善折叠比例均达标才保留。
