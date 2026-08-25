# 30 天 shadow Soak 与运行 SLO

更新时间：2026-08-23

当前 soak 结论：`BLOCKED`。监控代码和数据库指标已实现，但时间本身无法由单元测试替代。

## 通过条件

默认每 60 秒采样，连续运行至少 30 天：

| 指标 | 门槛 |
|---|---:|
| 覆盖时间 | >= 30 天 |
| 有效采样 | >= 40,000（30 天理论值 43,200） |
| 最大采样间隔 | <= 300 秒 |
| 不洁重启 | 0 |
| 未知订单 / 未知仓位 | 0 / 0 |
| 对账不一致 | 0 |
| 重复 order_link_id | 0 |
| receipt outbox backlog | 0 |
| stale source | 0 |
| 交易所时钟漂移 | 绝对值 <= 2 秒 |
| RSS 增长斜率 | <= 25 MiB/天 |
| Windows handle 增长斜率 | <= 20/天 |
| 日志增长斜率 | <= 100 MiB/天 |

另外记录线程数、执行库/WAL 大小、未完成票据、失败票据、WebSocket 重连次数。重连次数本身用于诊断；是否失败还要结合断线时长、漏消息和对账结果。

## 运行方式

交易服务启动时自动建立唯一 `service_run`，每分钟把运行指标写入 execution DB；正常退出记录 clean shutdown。若下次启动发现上次没有正常关闭，只计一次 unexpected restart。

查看结论：

```powershell
Set-Location D:\Money
python scripts\soak_status.py --db BybitContractBotV4\execution_state.sqlite3
```

不足 30 天、样本不足或任一 SLO 不满足时，命令返回 `BLOCKED` 是正确行为。

## 日志控制

交易日志写入 `BybitContractBotV4/logs/trading_service.log`。单文件 25 MiB，最多保留 14 个备份，总量上限约 375 MiB；相同消息每 60 秒最多放行 20 条，下一窗口会写出被抑制次数。key、secret、Bearer 和签名在 handler 前被替换为 `<redacted>`。

指标库不是日志替代品：日志轮换不影响 30 天 SLO，因为关键序列保存在 execution DB。生产机仍应由外部采集器读取健康端点和只读指标副本，并配置磁盘/进程级告警。

## 故障处置

| IncidentMode | 允许动作 |
|---|---|
| NORMAL | 在全部门禁健康时允许正常动作 |
| FREEZE_NEW_RISK | 禁止 OPEN/INCREASE/REPLACE，保留减仓/保护 |
| CANCEL_ENTRIES | 取消未成交入场，保留保护和减仓 |
| PROTECT_ONLY | 仅允许保护、REDUCE/CLOSE |
| FLATTEN | 只允许风险降低和平仓 |
| MANUAL_HANDOVER | 未知状态，禁止自动增加风险，等待明确人工处置/认领 |

任何未知仓位/订单、对账失败、时钟或数据源失效都不得通过填假值或手工改库解除。
