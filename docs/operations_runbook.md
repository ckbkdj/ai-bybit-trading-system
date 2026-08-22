# 部署与运行手册

更新时间：2026-08-22

## 1. 安装与配置

建议预测端和交易端使用独立 Python 虚拟环境。预测端安装 `ai_bot3/ai_bot3/requirements.txt`，交易端安装 `BybitContractBotV4/requirements.txt`。

复制 `BybitContractBotV4/.env.example` 为 `.env.local`。该文件已被 Git 忽略。至少配置：

- `BYBIT_TRADING_MODE=shadow`
- `BYBIT_POSITION_MODE=hedge` 或与账户一致的 `one_way`
- `TICKET_API_BASE_URL=http(s)://内网控制面地址`
- `TICKET_API_TOKEN` 与控制面的 `CONTROL_PLANE_API_TOKEN` 相同
- 内网 HTTPS 使用 `PREDICTION_CA_BUNDLE`，不要长期关闭校验

预测控制面通过进程环境读取：

```text
CONTROL_PLANE_DB=D:\安全数据目录\control_plane.sqlite3
RESEARCH_JOB_DB=D:\安全数据目录\research_jobs.sqlite3
CONTROL_PLANE_API_TOKEN=部署时注入
AI_BOT_TICKETS_ENABLED=true
AI_BOT_BRAIN_INFERENCE_STAGE=shadow
AI_BOT_REQUIRED_BRAIN_RELEASE_STAGE=live
```

API key、secret、token 和 webhook 只在安全机器上通过环境或 `.env.local` 注入。任何曾写入旧源码或日志的凭证均按已泄露处理并轮换。

## 2. 启动顺序

1. 启动预测控制面：在 `ai_bot3/ai_bot3` 下执行 `python -m uvicorn api.control_plane_main:app --host 内网IP --port 8000`。
2. 检查 `/v1/health` 和三份 `/v1/schema/...`。
3. 保持 `BYBIT_TRADING_MODE=shadow`，启动 `python bot_threshold_super_v4_1.py`。
4. 检查交易端 `http://127.0.0.1:8787/health`。
5. 运行 `python scripts/run_shadow_e2e.py`，确认票据、影子订单、回执都为 1。

服务顺序可以反过来启动；交易端的本地游标、执行库和回执 outbox 会在控制面恢复后继续。不要同时启动两个使用同一 consumer_id 和同一执行库的交易进程。

## 3. 运行观察

健康状态重点：

- 控制面 schema 版本和 `tickets_enabled`；
- 交易模式必须符合预期；
- kill switch 必须可见；
- testnet/live 的私有 WebSocket 必须 connected；
- incomplete ticket 数量不能持续增长；
- last error、last poll 和 receipt outbox 是否滞留；
- Brain inference/release stage、校准状态、OOD、数据质量和拒票原因；
- 账户净值高水位、当前 drawdown、日亏损和保证金；
- 机器 UTC 时间与 Bybit 时间漂移。

主循环不把 REST accepted 当作 FILLED。短暂 `SUBMITTING` 是正常恢复窗口；超过窗口且按订单号/成交查不到时进入 FAILED，需要人工核对，不能手工“再按一次下单”。

## 4. 故障处置

### 控制面或内网中断

新开仓票据因 data source health 失败而被拒绝；已有减仓/平仓仍允许通过风险降低路径。回执保留在本地 outbox，恢复后重发。

### 私有 WebSocket 中断

testnet/live 禁止风险增加。先恢复 WebSocket，再执行 reconcile；不要依据 REST 返回手工改为 FILLED。

### 未知下单/撤单结果

保留 SUBMITTING，用确定性 orderLinkId 查询实时订单、历史订单和成交。禁止改变 ticket_id 后重发同一意图。

### 止盈子单安装失败

入场单已有原子附带止损；系统同时打开 kill switch。人工核对持仓、止损和已存在的 reduce-only 子单，修复后才解除 kill。

### 日亏损、连续亏损或保证金门槛

不要修改数据库绕过风控。确认账户快照、PnL 日界线和敞口后再决定是否进入新的交易日或结束冷静期。

### 净值高水位回撤门槛

`equity_runtime` 跨重启保存高水位。达到 `MAX_EQUITY_DRAWDOWN_PCT` 后所有风险增加票据失败关闭；只能继续风险降低动作。先核对 Bybit 权益、未实现盈亏和数据一致性，不得删除/回写高水位绕过门禁。

## 5. 备份与恢复

停止写服务后，同时备份 SQLite 主文件、`-wal`、`-shm`。控制面、研究、PIT 和执行状态使用新数据库，不覆盖旧价格库、模型或结果 JSON。恢复时先复制数据库，再以 shadow 启动并运行 reconcile。

代码版本使用 `D:\Money\git-local.ps1`：

```powershell
.\git-local.ps1 status --short
.\git-local.ps1 log --oneline
.\git-local.ps1 diff
```

数据库下行脚本只在已有备份且明确放弃新数据时人工执行，详见 `docs/migration_rollback.md`。

## 6. 模式升级

升级顺序固定为 shadow → candidate testnet → live-model shadow → 小流量 live。candidate 只有同时设置 `AI_BOT_BRAIN_INFERENCE_STAGE=candidate` 和 `AI_BOT_REQUIRED_BRAIN_RELEASE_STAGE=candidate` 才能出测试票。生产保持 required stage 为 `live`。testnet 至少验证限价、市场价、部分成交、撤单、重启恢复、止损、止盈子单、限流和 WebSocket 断线。模型晋升使用证据包和 approval id；交易主网仍必须完成 `docs/mainnet_go_live_checklist.md` 并获得人工批准。
