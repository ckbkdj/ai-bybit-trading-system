# Versioned AI-to-Bybit Trading System

本目录现在包含一套默认 `shadow`、失败关闭、可回放的预测—执行系统。现用边界不再是旧预测 JSON 或共享数据库，而是三份不可变契约：`ForecastEnvelope v1`、`OperationTicket v1`、`ExecutionReceipt v1`。

## 入口

- 预测控制面：`ai_bot3/ai_bot3/api/control_plane_main.py`
- 交易执行服务：`BybitContractBotV4/bot_threshold_super_v4_1.py`
- 跨项目影子验收：`scripts/run_shadow_e2e.py`
- 系统架构：`docs/architecture_v1.md`
- 部署与故障处理：`docs/operations_runbook.md`
- 20 项验收矩阵：`docs/acceptance_report.md`
- 主网上线门禁：`docs/mainnet_go_live_checklist.md`

## 安全状态

默认模式为 `shadow`。`testnet` 和 `live` 均要求凭证；`live` 还要求显式 `BYBIT_ENABLE_LIVE=true`。本次实现和验证没有启用主网，也没有写入或删除旧数据库、模型和历史 bot。

## 快速验证

```powershell
# 交易侧离线测试
python -m unittest discover -s BybitContractBotV4\tests -v

# 预测侧新增核心测试（在 ai_bot3/ai_bot3 中运行）
python -m unittest tests.test_contracts_v1 tests.test_ticket_outbox tests.test_point_in_time_features tests.test_research_jobs tests.test_evaluation_protocol tests.test_provider_boundary -v

# 真实本机 HTTP、影子下单、回执闭环
python scripts\run_shadow_e2e.py
```

上线前请完整执行 `docs/mainnet_go_live_checklist.md`，不要把通过离线测试等同于已经获准实盘。
