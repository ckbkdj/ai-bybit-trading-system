# 使用与迁移手册 v2

更新时间：2026-08-23

## 先看当前状态

当前版本适合离线验证和 shadow，不适合直接放真钱。旧代码、旧库和模型均保留；新特征库也没有替换生产文件。请先用本文完成复核，再讨论是否按 v2 架构切换。

## 目录怎么读

```text
D:\Money
├─ ai_bot3\ai_bot3              预测、研究、控制面
│  ├─ core                      模型、PIT、校准、数据 provider
│  ├─ contracts                 三份版本化合同
│  ├─ api                       控制面和兼容 API
│  ├─ data                      旧库与新重建候选库
│  └─ model_results\evaluation 机器审计报告
├─ BybitContractBotV4           独立交易执行服务
│  ├─ service_main.py           活跃服务编排
│  ├─ exchange_gateway.py       最小交易所边界
│  ├─ legacy_experience.py      两年经验的可测试规范
│  └─ tests                     执行与风控回归
├─ scripts                      跨项目审计、重建、影子验收
└─ docs                         架构、证据、门禁和本手册
```

参考数据服务只读位置：`D:\lh\trad_data_service_20260821\data_service`。

## 1. 准备配置

预测端复制 `ai_bot3/ai_bot3/.env.example`，交易端复制 `BybitContractBotV4/.env.example` 为各自 `.env.local`。不要提交 `.env.local`。

影子模式的关键值：

```text
BYBIT_TRADING_MODE=shadow
BYBIT_ENABLE_LIVE=false
BYBIT_LIVE_APPROVAL_ID=
AI_BOT_TICKETS_ENABLED=true
AI_BOT_BRAIN_INFERENCE_STAGE=shadow
AI_BOT_REQUIRED_BRAIN_RELEASE_STAGE=live
```

可选启用参考日频面板：

```text
TRAD_DATA_SERVICE_ROOT=D:\lh\trad_data_service_20260821\data_service
TRAD_PANEL_VERIFY_SHA256=true
```

这只会允许只读 shadow context；不会让面板因子进入交易方向。

所有 Bybit、LLM、控制面 token 在安全机器上重新注入。旧配置曾出现过一枚 key，即使当前文件已清空，也应轮换旧值。

## 2. 先跑回归

在 `D:\Money` 执行：

```powershell
$env:PYTHONPATH='D:\Money\.test-deps;D:\Money;D:\Money\ai_bot3\ai_bot3'
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' -m pytest ai_bot3\ai_bot3\tests -q
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' -m pytest BybitContractBotV4\tests -q
& 'D:\lh\trad_data_service_20260821\data_service\.venv\Scripts\python.exe' scripts\run_shadow_e2e.py
```

本次基线应看到 112 passed、43 passed，以及 `shadow_order_count=1`、`control_plane_receipt_count=1`。

## 3. 启动 shadow

先启动预测控制面：

```powershell
Set-Location D:\Money\ai_bot3\ai_bot3
python -m uvicorn api.control_plane_main:app --host 127.0.0.1 --port 8000
```

再启动交易服务：

```powershell
Set-Location D:\Money\BybitContractBotV4
python bot_threshold_super_v4_1.py
```

检查：控制面 `/v1/health`、交易端 `http://127.0.0.1:8787/health`、模式为 shadow、kill switch 状态、票据积压、私有流状态和回执 outbox。

## 4. 查看证据

优先读这些文件：

- `docs/architecture_v2_readable.md`：模块边界和数据流。
- `docs/bug_and_evidence_audit.md`：发现、修复和残余风险。
- `docs/legacy_reuse_and_data_sources.md`：旧经验如何复用、因子从哪里来。
- `docs/research_and_validation_report.md`：市场研究、过拟合和测试结果。
- `ai_bot3/ai_bot3/model_results/evaluation/*.json`：机器可读原始报告。

## 5. 新特征库怎么处理

当前环境默认仍指向旧库：

```text
AI_BOT_KLINE_FEATURE_STORE_PATH=./data/kline_feature_store.sqlite3
```

旧库损坏时训练会失败关闭。重建候选是：

```text
./data/kline_feature_store.rebuilt.20260822.sqlite3
```

在我们讨论并批准前不要切。批准后的安全顺序：停止训练/预测写进程；同时备份 SQLite 主文件及可能存在的 `-wal/-shm`；把 `AI_BOT_KLINE_FEATURE_STORE_PATH` 改为 rebuilt；以 shadow 启动；核对 25 组时间范围、预测数量、特征 hash、延迟和资源；连续观察；若异常只需把变量改回旧值。不要覆盖或删除任何一个库。

## 6. testnet 到 live

固定顺序：

```text
shadow -> candidate testnet -> live-model shadow -> 小流量 live
```

testnet 必须覆盖限价/市价、部分成交、撤单、重启恢复、止损、止盈、最长持仓、限流、断线、对冲/单向仓位和真实交易所精度。

live 需要同时满足：模型工件 stage=live；证据包和 SHA 完整；人工批准；`BYBIT_TRADING_MODE=live`；`BYBIT_ENABLE_LIVE=true`；非空 `BYBIT_LIVE_APPROVAL_ID`；正确的新 key；健康门禁全部通过。任意一个缺失都应启动失败或禁止增加风险。

## 7. Git 与回滚

使用工作区本地 Git 包装器：

```powershell
.\git-local.ps1 status --short
.\git-local.ps1 diff
.\git-local.ps1 log --oneline
```

回滚代码不等于回滚数据。保留控制面/执行库和 `-wal/-shm`，先以 shadow 恢复并运行 reconcile。不要删除净值高水位或改数据库绕过风险。详细主网条件见 `docs/mainnet_go_live_checklist.md`。
