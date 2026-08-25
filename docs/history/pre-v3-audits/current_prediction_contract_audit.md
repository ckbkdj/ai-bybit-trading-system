# 当前预测契约审计

审计日期：2026-08-21  
范围：`D:\Money\ai_bot3\ai_bot3`  
性质：只读审计；未运行预测、未访问交易所、未修改业务代码。

## 结论

当前预测端没有正式、版本化的跨系统契约。实际边界是按币种和模式覆盖写入的 JSON 文件，以及 API 对这些文件的再次包装：

- 文件：`model_results/{SYMBOL}_{MODE}.json`
- 聚合接口：`GET /predict/{symbol}`
- 旧兼容接口：`GET /results/{symbol_}`

交易端当前调用旧兼容接口，并且只读取 `trend`。因此现在的接口不是“预测分布 → 决策 → 操作票”，而是“一个可变 JSON 中的方向字段 → 交易脚本中的技术规则”。

## 当前生成链路

1. `main_forecast.py:12-33` 从 `config.yml` 读取调度配置，并把每个模式放入单消费者队列。
2. `core/portfolio3_3_fixed.py:1139-1174` 为每个币种和模式准备数据，然后在子进程执行 Keras 推理。
3. `core/inferencer3_fixed.py:163-245` 生成价格、方向、收益、置信度和融合因子等字段。
4. `core/inferencer3_fixed.py:356-418` 再叠加 Brain 判断、目标杠杆和行情来源信息。
5. `core/portfolio3_3_fixed.py:1175-1239` 记录在线学习样本，并调用 `ResultManager` 保存结果。
6. `core/result_manager.py:99-110` 直接覆盖写入目标 JSON；预测结果没有 revision、不可变历史或发布事务。
7. `api/api_server.py:880-952` 读取 JSON，合并训练元数据、LLM 缓存和评估信息后返回。

## 当前模式

| mode | timeframe | 代码内结算周期 |
|---|---:|---:|
| `scalping` | `3m` | 180 秒 |
| `mid_short` | `15m` | 900 秒 |
| `trend` | `2h` | 7200 秒 |
| `trend_swing` | `4h` | 14400 秒 |
| `swing` | `1d` | 86400 秒 |

timeframe、mode 和 horizon 目前分散在配置、API 常量和 `portfolio3_3_fixed.py:1185-1188` 中，没有单一来源。

## 当前输出字段

本地样本包含约 70 个顶层字段，主要分为以下几组：

| 类别 | 当前字段示例 | 审计判断 |
|---|---|---|
| 身份 | `symbol`, `timeframe`, `model_version` | 缺 `forecast_id`、revision、schema version、代码提交号 |
| 时间 | `generated_at`, `saved_at`, `updated_at`, `latest_kline_ts` | 缺统一 `data_cutoff`、`valid_until`、目标时刻 |
| 点预测 | `pred`, `last`, `predicted_return`, `trend` | 有点预测，没有方向概率和收益分位数 |
| 误差 | `rmse`, `ci`, `score` | `ci` 是基于整体 RMSE 的简单区间，不是经过覆盖率验证的预测区间 |
| 融合 | `factor_bias`, `ensemble_score`, `fused_weights`, `calibrated_trend` | 研究结果和展示字段混在同一层 |
| Brain | `trade_actionable`, `target_leverage`, `target_raw_return` | 已带执行意味，但没有账户、成本、风险预算和有效期约束 |
| 数据质量 | `data_source_status`, `latest_kline_ts`, `current_price_warning`, `context_completeness` | 有局部质量信息，但没有统一门控结果或最老特征年龄 |
| 血缘 | `training_metadata`, `selected_params`, `model_version` | 缺不可变模型 bundle、feature set 和 calibration 版本关系 |

2026-08-21 的 BTC 五个模式样本均观察到 `target_leverage=100`。该值由预测端产生，但没有契约说明它是展示值、建议值还是执行上限，不能被交易端直接采用。

## API 行为差异

`GET /predict/{symbol}` 会调用 `_prediction_source_warning`，返回 `data_source_warning` 和 `data_source_reliable`。旧接口 `GET /results/{symbol_}` 主要做数值类型归一化，没有同等的数据可靠性门控。

交易端恰好使用 `/results/{symbol}`，所以预测端已经存在的一部分陈旧数据检查并没有进入交易决策边界。

API 还存在以下边界风险：

- `api_server.py:52-58` 允许任意来源跨域访问。
- 预测接口没有认证、签名、consumer 身份或重放保护。
- JSON 被原地覆盖，没有游标、outbox 或消费确认。
- API 读取失败时返回兼容形状，但没有标准错误契约。

## 数据质量现状

正向能力：

- 最新 Binance 拉取失败时，`data_fetch.py:166-210` 会拒绝用陈旧缓存继续预测。
- funding rate 和 long/short ratio 的关键失败会阻止本轮预测。
- API 能检查 K 线年龄与当前价来源。

缺口：

- 多数本地 Coinglass/新闻扩展列缺失时在 `inferencer3_fixed.py:524-532` 补 `0.0`，缺失与真实零值不可区分。
- 只有 `generated_at`/`fetched_at` 等零散字段，没有统一 `event_time`、`published_at`、`available_at`、`ingested_at`。
- 预测 JSON 没有稳定的 schema 校验；新增或重命名字段可能静默影响交易消费者。
- `ResultManager` 的预测文件不是原子替换写入，API 读取与写入并发时存在短暂半文件风险；训练元数据写入则使用临时文件替换。

## 阶段 1 应保留的兼容边界

阶段 1 不应立即删除现有 JSON。建议新增只读适配链：

`legacy result JSON -> LegacyForecastAdapter -> ForecastEnvelope v1`

适配器必须：

- 明确旧字段到新字段的映射；无法推导的字段保持 `null`，不得伪造。
- 为每次已保存预测生成稳定 `forecast_id` 和 revision。
- 记录 `source_schema="legacy-result-json"`。
- 将 `target_leverage` 仅作为 legacy evidence 保存，不赋予执行语义。
- 同时校验 JSON Schema 与 Pydantic 模型。
- 以 golden samples 固定当前五种模式的兼容结果。

在 OperationTicket 建立以前，任何 ForecastEnvelope 都只能用于记录和影子决策，不能触发真实订单。
