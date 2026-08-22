# Versioned AI-to-Bybit Trading System

本目录现在包含一套默认 `shadow`、失败关闭、可回放的预测—执行系统。现用边界不再是旧预测 JSON 或共享数据库，而是三份不可变契约：`ForecastEnvelope v1`、`OperationTicket v1`、`ExecutionReceipt v1`。

## 入口

- 预测控制面：`ai_bot3/ai_bot3/api/control_plane_main.py`
- 交易执行服务：`BybitContractBotV4/bot_threshold_super_v4_1.py`
- 跨项目影子验收：`scripts/run_shadow_e2e.py`
- 可读版系统架构：`docs/architecture_v2_readable.md`
- 完整 Bug 与证据审计：`docs/bug_and_evidence_audit.md`
- 旧版经验和真实数据来源：`docs/legacy_reuse_and_data_sources.md`
- 使用与迁移手册：`docs/user_guide_v2.md`
- 部署与故障处理：`docs/operations_runbook.md`
- 20 项验收矩阵：`docs/acceptance_report.md`
- 主网上线门禁：`docs/mainnet_go_live_checklist.md`

## 安全状态

默认模式为 `shadow`。`testnet` 和 `live` 均要求凭证；`live` 还同时要求显式 `BYBIT_ENABLE_LIVE=true` 和非空 `BYBIT_LIVE_APPROVAL_ID`。本次实现和验证没有启用主网，也没有删除旧数据库、模型和历史 bot。损坏特征库已非破坏重建为候选文件，但生产配置尚未切换。

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
