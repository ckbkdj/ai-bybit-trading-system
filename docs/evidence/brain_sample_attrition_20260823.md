# Brain 样本数与 258 万行特征库的差异证据

日期：2026-08-23

## 结论

“候选库有 2,587,737 行”和“旧强制重训只有 540–600 行”并不矛盾。总行数覆盖 5 个币种、5 个 timeframe 和完整时间范围；旧脚本绕过版本化特征库，从每个 symbol/mode 的短缓存表读取固定窗口。它统计的是单模型缓存切片，不是全库可训练样本。

## 旧训练历史只读统计

数据库 `ai_bot3/ai_bot3/data/brain_training_history.sqlite3`：

| status | 次数 | rows 最小 | rows 最大 |
|---|---:|---:|---:|
| trained | 10,591 | 720 | 13,143 |
| skipped_same_signature | 4,901 | 1,073 | 2,967 |
| skipped_insufficient_samples_or_classes | 20 | 540 | 600 |
| 合计 | 15,512 | 540 | 13,143 |

时间范围为 2026-05-12 至 2026-08-22。最后一批五个币种的 scalping/mid_short/trend_swing 为 540，swing 为 600，trend 为 720；这是旧 `retrain_brain_from_cache.py` 路径的固定短缓存行为。

## 当前版本化候选库的真实损耗链

审计按每个 symbol/mode 依次应用：指定窗口、未收盘过滤、one-bar PIT lag、未来标签 horizon、两处 purge gap、独立 validation/test。

| mode | symbol 数 | eligible 最小–最大 | train 最小 | validation 最小 | test 最小 | split |
|---|---:|---:|---:|---:|---:|---|
| scalping | 5 | 87,703–527,036 | 61,387 | 13,155 | 13,155 | 全部 ready |
| mid_short | 5 | 19,018–19,020 | 13,308 | 2,853 | 2,853 | 全部 ready |
| trend | 5 | 11,112 | 7,774 | 1,667 | 1,667 | 全部 ready |
| trend_swing | 5 | 6,585 | 4,605 | 988 | 988 | 全部 ready |
| swing | 5 | 1,096 | 766 | 164 | 164 | 全部 ready |

## 修复

- 重训脚本默认改为只读版本化 KlineFeatureStore，并在训练前输出语义 attrition gate。
- 旧缓存路径只有显式 `--legacy-cache-diagnostics` 才能运行，并且禁止训练或晋升，只能生成 shadow 诊断。
- feature identity 与 training split config 分离。改变 15/15 validation/test 比例不会再把相同的 258 万行派生特征误判为另一套特征。
- Brain 使用独立 train/validation/test 和两处 purge；独立 test 不达基线时最多保留 shadow，不能晋升。

这证明样本管线已被纠正，不证明模型会赚钱。任何新训练仍需 lockbox、费用、容量、多重试验和执行反馈验收。

机器证据：`docs/evidence/feature_store_semantic_audit_20260823.json`。
