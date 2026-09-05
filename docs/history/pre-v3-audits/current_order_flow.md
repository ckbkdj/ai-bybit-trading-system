# 当前订单流程

审计对象：`BybitContractBotV4/bot_threshold_super_v4_1.py` 与 `bybit.py`

## 运行循环

```text
loop_v4_1.sh
  -> 检查进程，不存在则启动 bot_threshold_super_v4_1.py
  -> main() 每 3 秒循环
      -> get_data()
          -> 读取 Bybit 余额
          -> 计算每币种基础保证金 = 总权益 × 0.006
          -> 遍历 setting_v4.ini 中的币种
              -> get_price()
      -> set_tracking_exit()
```

没有任务队列、操作票消费者、lease、游标或 outbox。

## 入场流程

```text
读取 per-symbol switch
  -> 拉取当前 Bybit 仓位
  -> GET 外部 /results/{symbol}
  -> 固定读取 XRPUSDT/scalping/trend
  -> 拉取 1 秒、1 分钟和配置周期 K 线
  -> 计算 EMA、布林带和大量规则信号
  -> 检查未完成订单
  -> 空仓：技术信号 + trend 决定 long/short
  -> BybitClient.create_order()
  -> 立即更新 setting_v4.ini 的时间、次数和价格
```

预测不是一条独立可追踪的决策记录，订单也不能反向定位到触发它的预测和技术条件。

## 下单分支

### `go_long` / `go_short`

根据蜡烛振幅和短期形态选择不同价格。多数分支创建限价单，少数分支使用 market。创建后等待约 3 秒并继续轮询。

### 已有一边仓位

反向信号可能直接创建另一边订单。当前系统运行在 hedge position index 语义下，不是简单的单向净仓位状态机。

### 加仓

`lock_profits` 允许盈利加仓和亏损/信号加仓。加仓金额会根据现有 size、基础金额、杠杆和加仓次数重新计算；提交后立刻更新 INI 计数。

### 止盈止损

- 初始入场通常不携带止盈止损。
- 后续轮询调用 Bybit trading-stop API 设置或修改 Full TPSL。
- 还会创建一至多张反向限价单作为分批止盈。
- 进程级 `set_tracking_exit` 可能启用或取消 trailing stop。

## 当前隐含状态

代码没有显式订单状态枚举，实际状态分散在：

- Bybit `fetch_open_orders` 返回值。
- Bybit `fetch_positions` 返回值。
- `setting_v4.ini` 的 buy/sell count、time、position、takeprofit 标记等字段。
- 当前函数调用是否抛异常。

当前无法可靠区分：

- 已发送但 REST 响应丢失。
- 已受理但未成交。
- 部分成交。
- 已成交但仓位接口尚未更新。
- 撤单请求与成交同时发生。
- 重启前已提交、重启后尚未 reconcile。

## 失败窗口

| 窗口 | 当前结果 |
|---|---|
| 交易所受理后网络超时 | 本地可能重试并重复下单 |
| REST 返回后、INI 更新前崩溃 | 订单存在，本地计数未更新 |
| INI 写入中进程崩溃 | ConfigParser 直接覆盖，文件可能不完整 |
| 入场成交后、下一轮保护前故障 | 仓位可能无止损 |
| 限价平仓部分成交 | 没有 fill 累计和剩余数量账本 |
| cancel 与 fill 竞争 | 只看轮询结果，没有事件顺序控制 |
| 预测响应陈旧或被篡改 | 仍可能参与主网入场判断 |

## 当前恢复

重启脚本只恢复进程，不恢复工作单元。Python 主循环重新读取 INI、余额、仓位和未完成订单，然后继续执行规则。

没有执行日志可以回答：

- 某笔订单为何产生。
- 是否已经被消费过。
- 对应哪次预测。
- 是否发生过部分成交、重复通知或回放。
- 哪次重启接管了该订单。

## 目标状态机映射建议

阶段 3 可把现状逐步映射为：

```text
RECEIVED -> VALIDATED -> CLAIMED -> RISK_APPROVED
         -> SUBMITTING -> SUBMITTED -> ACKNOWLEDGED
         -> PARTIALLY_FILLED -> FILLED
```

终态：`REJECTED`、`EXPIRED`、`RISK_BLOCKED`、`CANCELLED`、`FAILED`、`SUPERSEDED`。

迁移时必须先影子记录新状态，不得直接替换现有执行路径。待重复票、部分成交、cancel/fill、重启恢复和 no-real-order 测试全部通过后，才考虑受控切换。
