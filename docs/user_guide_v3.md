# Shadow 候选使用、验证与回滚手册 v3

更新时间：2026-08-23

当前版本只按 **可部署的 shadow 工程候选，持续运行能力仍需长时间 soak 验证** 使用。不要配置真钱权限，也不要把测试通过当作盈利证明。

## 目录入口

```text
D:\Money
├─ ai_bot3\ai_bot3
│  ├─ core\kline_feature_store.py     版本化特征/PIT/attrition
│  ├─ core\decision                  SignalBook、PortfolioIntent、TicketBuilder
│  ├─ core\release                   StrategyReleaseBundle 校验
│  ├─ core\evaluation                时间切分、执行标签
│  ├─ contracts                      六类版本化合同和 JSON Schema
│  └─ api\control_plane_main.py      预测控制面
├─ BybitContractBotV4
│  ├─ service_main.py                独立执行服务入口
│  ├─ ticket_store.py                execution DB、epoch、命令/订单账本
│  ├─ incident_modes.py              故障动作策略
│  ├─ soak_monitor.py                长稳指标
│  └─ exchange_gateway.py            最小 Bybit 边界
├─ scripts                           审计、E2E、soak、供应链工具
└─ docs                              架构、证据、门禁、手册
```

参考服务严格只读：`D:\lh\trad_data_service_20260821\data_service`。

## 1. 安全配置

从两个 `.env.example` 复制为各自 `.env.local`，只在安全机器注入 key。Git 已忽略 `.env.local`。

shadow 最小设置：

```text
BYBIT_TRADING_MODE=shadow
BYBIT_ENABLE_LIVE=false
BYBIT_DEDICATED_SUBACCOUNT=false
AI_BOT_TICKETS_ENABLED=true
AI_BOT_STRATEGY_RELEASE_BUNDLE=<经过校验的 shadow bundle 路径>
AI_BOT_KLINE_FEATURE_STORE_PATH=./data/kline_feature_store.sqlite3
APP_CODE_COMMIT=<本次 Git commit>
```

没有 bundle 时预测可以落库，但不会形成组合工单。这是失败关闭，不是故障。

## 2. 回归与证据复核

预测端：

```powershell
Set-Location D:\Money
$env:PYTHONPATH='D:\Money\.test-deps;D:\Money;D:\Money\ai_bot3\ai_bot3'
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' -m pytest -q ai_bot3\ai_bot3\tests
```

交易端：

```powershell
Set-Location D:\Money\BybitContractBotV4
$env:PYTHONPATH='D:\Money\.test-deps;D:\Money;D:\Money\BybitContractBotV4'
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' -m pytest -q
```

跨进程影子闭环：

```powershell
Set-Location D:\Money
$env:PYTHONPATH='D:\Money\.test-deps;D:\Money;D:\Money\ai_bot3\ai_bot3;D:\Money\BybitContractBotV4'
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' scripts\run_shadow_e2e.py
```

当前证据应分别为 121 passed、59 passed，以及 `state=SUBMITTED`、`shadow_order_count=1`、`control_plane_receipt_count=1`。

## 3. 候选特征库验收

候选文件：`ai_bot3/ai_bot3/data/kline_feature_store.rebuilt.20260822.sqlite3`。它没有覆盖旧文件，生产路径也没有自动切换。

```powershell
Set-Location D:\Money
$env:PYTHONPATH='D:\Money\.test-deps;D:\Money;D:\Money\ai_bot3\ai_bot3'
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' scripts\audit_feature_store_semantics.py --db ai_bot3\ai_bot3\data\kline_feature_store.rebuilt.20260822.sqlite3 --output docs\evidence\feature_store_semantic_audit_20260823.json
```

只有 `deployment_status=PASS` 才能讨论切换；仍须先同时备份 SQLite 主文件和 `-wal/-shm`，停止写进程，修改环境路径后只以 shadow 对比。不要覆盖或删除旧库。

## 4. 启动 shadow

先启动控制面，再启动执行服务：

```powershell
Set-Location D:\Money\ai_bot3\ai_bot3
python -m uvicorn api.control_plane_main:app --host 127.0.0.1 --port 8000
```

```powershell
Set-Location D:\Money\BybitContractBotV4
python bot_threshold_super_v4_1.py
```

启动后检查：mode=shadow、release id、kill switch、incident mode、reconciliation、ticket/receipt backlog、来源健康、最后预测/轮询时间。日志位于 `BybitContractBotV4/logs/trading_service.log`。

## 5. 30 天 soak

不要重建 execution DB，否则连续证据会丢失。随时查看：

```powershell
Set-Location D:\Money
python scripts\soak_status.py --db BybitContractBotV4\execution_state.sqlite3
```

未达到 30 天返回 BLOCKED 是正确结果。完整门槛见 `docs/soak_slo.md`。

## 6. testnet 门禁

本轮没有使用任何真实 key，testnet 状态是 GATED。准备专用子账户后必须设置：

```text
BYBIT_TRADING_MODE=testnet
BYBIT_DEDICATED_SUBACCOUNT=true
POSITION_OWNER_ID=<固定且唯一的 owner>
BYBIT_ALLOW_MANUAL_ORDERS=false
APPROVED_STRATEGY_RELEASE_ID=<与 bundle 完全一致>
BYBIT_API_KEY=<安全注入>
BYBIT_SECRET_KEY=<安全注入>
```

必须实际覆盖 OPEN/INCREASE/REDUCE/CLOSE/CANCEL/REPLACE、部分成交、撤单成交竞争、私有 WS 断线、时钟漂移、限流、重启接管、未知订单/仓位和 TP/SL。未知状态应进入 `MANUAL_HANDOVER`，不得直接修改数据库绕过。

## 7. 供应链检查

```powershell
Set-Location D:\Money
python scripts\supply_chain_audit.py --output docs\evidence\supply_chain_audit_20260823.json
```

当前预期是 BLOCKED，因为依赖 lock 和漏洞 attestation 未完成。先在隔离构建环境解决，再生成 release bundle。

## 8. 回滚

- 代码：使用 `git-local.ps1` 查看提交并回到已批准版本；不要使用会删除用户数据的强制 reset。
- 特征库：停止写进程，把路径改回旧库；不覆盖两个库。
- 执行：先保持/开启 kill switch，保留 execution DB、WAL 和日志；重启后必须 reconcile 完成再允许动作。
- 模型：切回完整旧 bundle，不允许只替换某个模型/scaler 文件。
- 未知仓位：保持人工接管，只通过明确 approval 的 adoption/清理流程处理。

主网始终需要新的明确人工批准、完整 release、testnet、soak、供应链和盈利门禁；本手册不构成主网授权。
