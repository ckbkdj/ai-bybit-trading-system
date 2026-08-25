# 当前交易契约审计

审计日期：2026-08-21  
范围：`D:\Money\BybitContractBotV4`，主审 `bot_threshold_super_v4_1.py` 与 `bybit.py`  
性质：静态只读审计；为避免主网副作用，没有导入或运行任何交易模块。

## 结论

当前交易端不存在 OperationTicket，也没有可验证的预测消费契约。`bot_threshold_super_v4_1.py` 每约 3 秒轮询账户与市场，拉取预测 API，仅把 `trend` 当作本地技术信号的方向门槛，然后直接调用 CCXT 下单。

当前实现不满足安全上线所需的幂等、票据有效期、账户版本、成本门槛、订单状态机、私有 WebSocket 成交确认和重启恢复要求。

## 当前输入契约

`bot_threshold_super_v4_1.py:525-560` 请求：

`GET https://crypto_api.hk.ie520.com/results/{symbol}`

函数实际硬编码读取：

`payload["XRPUSDT"]["scalping"]`

随后只使用 `predicted_data["trend"]`，允许值在实践中为 `up`、`down` 或 `flat`。

这产生两个直接问题：

1. 调用 ETH、DOGE、1000PEPE 等 URL 时仍然寻找 `XRPUSDT`，非 XRP 币种通常会被判断为预测缺失并跳过。
2. 没有读取 `generated_at`、`data_source_reliable`、`confidence`、预测收益、成本、有效期或模型版本。

HTTP 调用还使用 `verify=False`，同时没有响应签名或认证。攻击者或错误代理只需改变一个 `trend` 字段，就可能影响真实下单方向。

## 当前交易决策

### 空仓

`bot_threshold_super_v4_1.py:824-853` 将本地 K 线信号和预测方向组合：

- 本地卖出条件成立且 `trend == "down"`：调用 `go_short`。
- 本地买入条件成立且 `trend == "up"`：调用 `go_long`。

### 单边持仓

`bot_threshold_super_v4_1.py:854-930` 在已有一边仓位时，反向技术信号和反向预测可以创建另一边仓位，形成 hedge mode 双向持仓。

### 已有持仓管理

`lock_profits` 负责盈利加仓、亏损加仓、动态止盈止损、追踪止损和多档限价平仓。该函数不接收预测对象；持仓后的大部分管理由本地技术条件、收益率和 INI 状态决定。

因此预测端当前只参与部分入场/对冲判断，并不真正提供完整操作意图。

## 当前仓位计算

`bot_threshold_super_v4_1.py:1853-1879`：

- 读取 Bybit 当前总权益、可用余额和盈亏。
- 每轮将 `total_usdt * 0.006` 作为每个币种的基础保证金金额。
- 杠杆来自 `setting_v4.ini` 的币种配置。

`bybit.py:65-123`：

`quantity = usdt_cost * leverage / price`

缺少以下组合风控：

- 总杠杆和相关资产总暴露上限。
- 日亏损上限与连续亏损冷静期。
- 价差、滑点、实时价格偏离和资金费率门槛。
- 交易所最小数量、数量步长和价格 tick 的显式规范化证据。
- position version 或账户快照版本。
- 预测收益扣除费用后的安全边际。

## 当前订单提交

- `BybitClient` 在模块导入阶段以真实凭证创建 CCXT 客户端并加载市场。
- 没有 testnet/shadow 默认开关。
- 使用 cross margin 和 hedge `positionIdx`。
- 全局 `ordertype="market"`，但大量调用不传该参数，因而使用 `BybitClient.create_order` 的默认 `limit`；部分分支显式传入 market。
- 新开仓通常传入 `stop_loss_percentage=None` 和 `take_profit_percentage=None`，初始订单没有随单保护；持仓后由后续循环尝试补充。
- 没有 `ticket_id`、`orderLinkId`/client order id 或幂等键。
- 调用者通常不检查返回订单内容，REST 返回后立即更新 INI 中的计数和时间。

## 当前确认与恢复

当前确认方式只有：

- 轮询 `fetch_open_orders`，按时间或新信号取消未完成限价单。
- 轮询 `fetch_positions`，根据是否出现持仓间接推断成交。

不存在：

- 私有订单/成交 WebSocket。
- `execId` 去重。
- 部分成交累计。
- cancel/fill 竞争处理。
- 单调订单状态机。
- REST 成功但结果未知时的 reconcile。
- 重启后未完成订单清单和恢复状态。

启动脚本只检查进程是否存在并重新拉起。重启时依赖交易所当前仓位和 `setting_v4.ini` 的可变字段恢复，无法证明本地状态与交易所一致。

## 当前持久化

`setting_v4.ini` 实际承担交易状态库角色，保存价格线、计数、加仓状态、止盈标记和最后操作时间。`price_changes.db` 只保存账户总值快照，不保存订单和成交。

没有保存：预测 ID、意图 ID、订单 ID、成交 ID、手续费、滑点、仓位版本或执行回执。

## 关键安全发现

### P0：明文凭证

主版本和多个历史版本包含明文 Bybit 凭证，通知 webhook 也在源码及启动脚本中。必须视为已经泄露：撤销并重新生成，之后只允许从环境变量或密钥服务读取。文档未记录任何具体值。

### P0：不可信预测可影响主网订单

预测来自固定外部地址，TLS 校验被关闭，没有认证、签名、schema、过期检查或重放保护。

### P0：默认主网并存在导入副作用

导入主脚本会初始化真实交易客户端。测试或工具只要 import 该模块，就可能连接主网；这使普通单元测试也不安全。

### P1：没有执行幂等与成交状态机

网络超时、进程崩溃或重启都可能造成“交易所已受理、本地未知”，随后重复提交。

### P1：开仓与保护之间存在时间窗口

入场订单通常不带止损，保护依赖后续轮询。程序、网络或 API 在窗口内故障时，仓位可能没有保护。

### P1：平仓限价单缺少清晰的 reduce-only 约束

`create_limit_liquidation_order` 使用反向订单和 `positionIdx`，但未显式设置 `reduceOnly`，也没有记录累计平仓数量与剩余仓位。

## 实施前硬门槛

1. 撤销现有 API key 和 webhook，清理所有历史脚本中的明文值。
2. 备份并建立 Git 基线；数据库和模型文件不得直接纳入普通代码提交。
3. 把客户端初始化移出 import 阶段。
4. 增加硬编码默认 `shadow=true`、`testnet=true`；启用主网必须显式双重确认。
5. 建立 fake exchange 和 no-real-order 测试夹具后，才允许修改订单路径。
6. 阶段 1 只消费影子 OperationTicket，不改变当前真实下单结果。
