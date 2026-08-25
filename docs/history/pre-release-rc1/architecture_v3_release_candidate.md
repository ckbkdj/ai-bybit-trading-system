# 量化平台产品架构 v3

> 盈利与模型发布部分已由 `docs/profitability_first_alpha_rebuild.md` 取代。本文保留预测—交易合同、恢复和执行架构历史；当前所有 Brain 均为 baseline-only/rejected，不能再按旧方向准确率门槛晋升。

更新时间：2026-08-25
当前结论：**Profitability Gate 未通过；仅保留 shadow/研究能力，0 candidate、0 live。**

这不是主网上线批准，也不是盈利证明。系统当前的目标是把两年线上经验保留下来，同时用可版本化、可恢复、可审计的边界替代旧脚本间的隐式耦合。

结构整理后的代码边界：`shadow_contracts/` 是 OperationTicket 与 ExecutionReceipt 的唯一实现，两端 `contracts/` 只是兼容入口；盈利流水线由 `profitability_rebuild.py` 编排、`profitability_rebuild_components.py` 承载可复用组件；Bybit public PIT 分为 `bybit_public_pit_store.py`、`bybit_public_pit_collector.py` 和 `bybit_public_pit_audit.py`，旧模块名保留为兼容门面。

## 一张图看完整闭环

```mermaid
flowchart TB
  subgraph DS[数据与证据源]
    BIN[Binance Futures\n预测市场 OHLCV / funding / OI / 多空 / taker]
    CGL[本地 Coinglass wrapper\n爆仓图 / 衍生品结构]
    EVT[事件与情绪\nTier A / B / C 分级]
    PANEL[trad_data_service\n跨资产日频面板，只读 shadow]
    BYM[Bybit V5\n盘口 / 成交 / instrument / 时钟]
    BYA[Bybit 专用子账户\n余额 / 仓位 / 订单 / fills]
    LLM[内网 LLM\n仅结构化辅助]
  end

  subgraph DATA[点时数据与特征]
    PIT[event / published / available / ingested 时间]
    FS[版本化特征库\nraw + enhanced + hash + PIT]
    HEALTH[数据健康\n缺失 / 陈旧 / range guard]
  end

  subgraph PRED[预测与组合决策]
    MODELS[LSTM + Brain + 在线校准]
    FE[ForecastEnvelope[]\n五个 horizon 独立预测]
    SB[SignalBook\n每周期只取最新合法版本]
    PI[PortfolioIntent\n净额化 / 风险预算 / 换手上限]
    OP[OrderPlan]
    TIX[OperationTicket[]\nOPEN / INCREASE / REDUCE / CLOSE / CANCEL / REPLACE]
    REL[StrategyReleaseBundle\n代码、模型、scaler、校准、特征、成本、证据、批准 SHA]
  end

  subgraph CTRL[控制面]
    OUT[SQLite WAL + immutable outbox\n版本迁移 / checksum / cursor]
    CLAIM[lease + claim_epoch fencing]
  end

  subgraph EXEC[独立 Bybit 执行服务]
    OWN[发布 ID 白名单 + 子账户 ownership]
    REC[reconcile-complete gate]
    INC[IncidentMode\n冻结新风险 / 取消入场 / 仅保护 / 平仓 / 人工接管]
    RISK[账户与组合终审\n回撤 / 杠杆 / 保证金 / basis / 时钟 / WS]
    IDEM[确定性 order/command id\nentry / TP / stop / close / cancel / revision]
    EX[Bybit REST + private WS]
  end

  subgraph LEARN[执行反馈与治理]
    RCPT[ExecutionReceipt]
    LABEL[ExecutionAwareLabel\nfill ratio / TTF / MFE / MAE / fee / slip / funding]
    COST[按 symbol/side/order/notional/spread/depth/session/regime/fee tier 的成本模型]
    GATE[OOS / lockbox / DSR / PBO / 容量 / 人工晋升]
  end

  BIN --> PIT
  CGL --> PIT
  EVT --> PIT
  PANEL --> PIT
  LLM --> PIT
  PIT --> FS --> HEALTH --> MODELS --> FE --> SB --> PI --> OP --> TIX
  REL --> MODELS
  REL --> TIX
  TIX --> OUT --> CLAIM --> OWN --> REC --> INC --> RISK --> IDEM --> EX
  BYM --> RISK
  BYA --> OWN
  BYA --> REC
  EX --> RCPT --> OUT
  RCPT --> LABEL --> COST --> GATE --> REL
```

## 决策语义

旧路径允许不同周期各自产生动作，容易同币种互相打架。v3 的唯一正式路径是：

```text
ForecastEnvelope[] -> SignalBook -> PortfolioIntent -> OrderPlan -> Ticket[]
```

- `SignalBook` 按 symbol、release、horizon 保存最新合法预测。
- 至少两个 horizon 才能形成 `PortfolioIntent`；各周期先贡献目标，再在组合层净额化。
- `PortfolioIntent` 显式记录净/多/空目标、风险预算、换手上限、贡献来源和唯一 hedge owner。
- 交易端不理解预测内部字段，只执行严格的 discriminated-union `OperationTicket`。
- 单预测出票函数仅保留为兼容与测试 fixture，不是正式生产决策入口。

## 发布单元

`StrategyReleaseBundle` 是一次晋升的最小单位，不允许只换模型文件：

| 必须绑定的内容 | 目的 |
|---|---|
| Brain、LSTM、scaler、calibration | 防止模型与预处理错配 |
| feature schema、factor contract | 防止列顺序、语义或来源悄悄变化 |
| cost model、ticket policy、execution policy | 防止回测和执行使用不同成本/规则 |
| code commit、training config、evidence | 能复现“这次为什么被批准” |
| approval id、manifest hash、每个 artifact SHA-256 | 防止文件替换或越权晋升 |

预测端没有经过哈希验证的 bundle 时只保存 forecast，不产生 portfolio ticket。测试网和主网交易端还必须配置完全相同的 `APPROVED_STRATEGY_RELEASE_ID`，否则在风险计算前拒绝工单。

## 数据市场与执行市场

- Binance 是当前主要预测市场，提供历史 K 线和部分衍生品结构。
- Bybit 是唯一执行市场；账户、仓位、订单、成交、精度、盘口、时钟和最终价格必须以 Bybit 为准。
- 短周期票据要求 Binance/Bybit basis 证据；缺失或超过阈值时失败关闭。
- Binance OI、多空和 taker 官方历史窗口有限，不能声称已拥有完整三年结构历史。

## 故障和所有权

测试网/主网只允许专用子账户：禁止人工订单，`POSITION_OWNER_ID` 固定。启动顺序是先冻结新风险，再查询全部未完成订单和仓位、恢复确定性订单、核对 ownership，最后才标记 `reconciliation_complete=true`。未知订单或未知仓位会进入 `MANUAL_HANDOVER`、打开 kill switch，并阻止增加风险；系统不会自动“认领”不明仓位。

租约使用服务实例级 token。新进程接管时 `claim_epoch` 单调增加；旧进程即使晚到，也无法预留订单或命令。entry、TP、stop attachment、close、cancel、revision 和最长持仓退出都具有由 ticket+role 派生的确定性 ID。

## 与旧版的关系

旧 v4.1 没有被整体删除。Git 锚点 `3f36b65` 保存原实现，两年经验被拆成：

- 安全不变量：止损只收紧、最长等待、退出保留、BTC/regime 过滤思想。
- 待重新验证假设：分档锁盈、分次入场、盈利加仓、波动带退出。
- 淘汰的实现：共享 INI、直接解释预测 JSON、REST 受理即成交、撤保护后加仓、全局强杀 Python。

这样复用的是经验和可检验假设，而不是把旧故障原样搬进新服务。

## 当前耦合度

| 边界 | 当前评价 | 残余债务 |
|---|---|---|
| 预测与交易 | 低：只经版本化合同、HTTP/outbox、receipt 交互 | 两端各有合同模型副本；已有 schema parity 回归，仍应纳入正式 CI |
| 数据与模型 | 中低：feature schema/hash、PIT、只读审计已建立 | `portfolio3_3_fixed.py` 仍较大；部分旧 collector 来源元数据不完整 |
| 模型与发布 | 中低：bundle 原子绑定并失败关闭 | 尚无真实 candidate/live bundle；依赖环境也未完全锁定 |
| 执行与交易所 | 低：最小 gateway、账本、恢复、ownership | 真实 Bybit testnet 尚未验收；DCP/重连外部证据缺失 |
| 研究与生产 | 中低：candidate/release 分离 | 30 天 soak、lockbox 盈利、执行成本样本尚未完成 |

可读性上，正式入口已经明确；残余大型旧模块应继续渐进拆分，不建议为了目录美观重写并丢失两年行为语义。

## 当前发布判断

架构可以继续作为下一轮讨论和 shadow 部署方向，但主网仍是 `NO-GO`。阻断项包括真实 testnet、30 天连续 soak、完整依赖锁与漏洞证明、独立 lockbox 费用后收益、执行回执校准成本模型，以及人工批准的 candidate/live release bundle。
