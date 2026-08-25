# 分阶段实施清单

更新时间：2026-08-25

## 已完成阶段

1. 审计：预测字段、交易读取、下单点、数据库、恢复缺口和安全阻断项已落文档。
2. 安全基线：密钥外置、默认 shadow、live 双开关、客户端延迟初始化、TLS/CA、原子配置写。
3. 契约：三份 v1 契约、JSON Schema、严格校验、legacy adapter 和 golden test。
4. 影子票据：成本后门槛、不可变预测/票据、游标 outbox、claim lease。
5. 执行：幂等订单号、状态机、私有流、成交去重、撤单、恢复、回执 outbox、保护订单。
6. 风控：实时账户/仓位/市场/健康校验、精度换算、日亏损/冷静期/敞口/kill switch。
7. PIT 与因子：时间四元组、vintage、质量/源中断、正式注册表、八状态图、可配置 JSON provider。
8. 慢研究：持久任务、checkpoint、revision、来源分级、去重、实体、情景和 EventImpactVector。
9. 评估：walk-forward/purge/embargo、成本与部分成交、预测/交易指标、因子组消融。
10. 服务与验证：薄控制面、薄交易入口、本机 HTTP 影子 E2E、20 项验收和运维/主网门禁。
11. 模型治理：purged holdout、训练段 scaler、研究 trial ledger、Deflated Sharpe、显式模型 stage 与人工证据晋升。
12. AI 到出票：Brain 正式方向、在线校准状态、range guard、来源可靠性、因子组得分和默认 live-only 门禁。
13. 组合证据与风控：历史非重叠成本复放、证据缺口报告、持久净值高水位和最大回撤熔断。
14. 工程整形：普通 clone CLI、报告归档、三层环境模板、runtime data manifest、平台锁 CI、Windows Python 3.12、盈利/PIT 拆分和跨服务共享合同。

## 有意保留且未自动执行

- 历史 bot、旧数据库、模型和结果文件未删除或批量改写。
- 用户的真实内网 provider URL、CA、token 和 Bybit key 未写入代码；由安全机器部署时注入。
- 未发起 Bybit testnet 或主网订单。testnet 外部验收和主网人工批准属于部署门禁，不是本地实现自动化可以替代的步骤。
- 当前控制面为单机 SQLite WAL；当出现多个写 worker 或高频写入时，再按 repository 边界迁移 PostgreSQL。

## 活跃文件边界

交易入口 `bot_threshold_super_v4_1.py` 仅启动 `service_main`。历史 v2/v3/v4/v6 保持只读。预测端原有模型与结果流程继续存在，`ResultManager` 在兼容适配后把标准预测与票据发布到新控制面；模型不直接接触交易所。`shadow_contracts/` 是跨服务票据/回执唯一实现；盈利重建和 Bybit PIT 的旧模块名只作为兼容入口。
