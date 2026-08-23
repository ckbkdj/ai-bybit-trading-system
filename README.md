# Versioned AI-to-Bybit Trading System

本目录现在包含一套默认 `shadow`、失败关闭、可回放的预测—执行系统。现用边界不再是旧预测 JSON 或共享数据库，而是版本化的 Forecast、PortfolioIntent、OperationTicket、ExecutionReceipt、ExecutionAwareLabel 和 StrategyReleaseBundle 契约。

当前结论：**Profitability Gate 未通过；仅保留 shadow/研究能力，0 candidate、0 live。** 工程测试通过不代表盈利，也不代表真钱许可。

## 入口

- 预测控制面：`ai_bot3/ai_bot3/api/control_plane_main.py`
- 交易执行服务：`BybitContractBotV4/bot_threshold_super_v4_1.py`
- 跨项目影子验收：`scripts/run_shadow_e2e.py`
- 盈利优先 Alpha 架构、因子来源与使用：`docs/profitability_first_alpha_rebuild.md`
- 可读版系统架构：`docs/architecture_v3_release_candidate.md`
- 部署就绪度：`docs/deployment_readiness_20260823.md`
- 完整 Bug 与证据审计：`docs/bug_and_evidence_audit.md`
- 旧版经验和真实数据来源：`docs/legacy_reuse_and_data_sources.md`
- 使用与迁移手册：`docs/user_guide_v3.md`
- 30 天 soak/SLO：`docs/soak_slo.md`
- 供应链门禁：`docs/supply_chain_gate.md`
- 部署与故障处理：`docs/operations_runbook.md`
- 验收矩阵：`docs/acceptance_report.md`
- 主网上线门禁：`docs/mainnet_go_live_checklist.md`

## 安全状态

默认模式为 `shadow`。所有旧 Brain 模型均为 baseline-only/rejected，不能独立出票。只有未参与调参的 lockbox 在全部成本后通过盈利、回撤、bootstrap、2x 成本、fold 稳定性、集中度、因子消融和执行证据门禁，才会生成 candidate manifest；当前没有生成。`live` 双开关未修改也未启用，旧数据库、模型、策略和历史 bot 均未删除。

## 快速验证

```powershell
# 预测侧全量测试
python -m pytest ai_bot3\ai_bot3\tests -q

# 交易侧全量测试
python -m pytest BybitContractBotV4\tests -q

# 真实本机 HTTP、影子下单、回执闭环
python scripts\run_shadow_e2e.py
```

上线前请完整执行 `docs/mainnet_go_live_checklist.md`，不要把通过离线测试等同于已经获准实盘。
