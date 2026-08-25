# CODEX 停止现场报告（2026-08-25）

## 1. 执行范围与状态

- 模式：受控收尾模式；本次仅完成本地只读盘点并生成本报告。
- 当前工作分支：`codex/complete-profitability-alpha-v2`
- 当前 HEAD：`f4a424fc06643e7af40478be5fc2c4d935a8491b`
- 最新本地提交：`f4a424f Recompute feature age at ticket boundary`
- 远端目标分支名称：`origin/codex/complete-profitability-alpha-v2`（按当前本地工作分支名称记录；本次禁止外网，未连接 GitHub 核验 remote ref）
- Draft PR #4：按既定要求保持 Draft、open、unmerged；本次未访问 GitHub，未修改或核验其远端状态。
- Issue #3：无限审计和开发循环不得继续；本次未继续。

## 2. Writer / Collector 进程

只读执行 `Get-CimInstance Win32_Process`。进程清单中未出现 `python.exe`，也未出现以下目标名称；因此本次快照判定不存在对应 writer PID：

| 目标 | Writer PID |
|---|---:|
| `run_profitability_rebuild` | 不存在 |
| `backfill_bybit` | 不存在 |
| `run_bybit_public_pit_collector` | 不存在 |

没有启动或停止任何进程。

## 3. SQLite 文件现场

目录：`D:\Money\ai_bot3\ai_bot3\data`

| 数据库/伴随文件 | 大小（bytes） | 约合 | 最后修改时间 | 状态 |
|---|---:|---:|---|---|
| `research_trials.sqlite3` | 131,072 | 128 KiB | 2026-08-24 22:06 | 存在 |
| `research_trials.sqlite3-wal` | — | — | — | 不存在 |
| `research_trials.sqlite3-shm` | — | — | — | 不存在 |
| `bybit_public_pit.prelockbox.sqlite3` | 8,724,013,056 | 约 8.125 GiB | 2026-08-25 00:12 | 存在 |
| `bybit_public_pit.prelockbox.sqlite3-wal` | — | — | — | 不存在 |
| `bybit_public_pit.prelockbox.sqlite3-shm` | — | — | — | 不存在 |
| `bybit_public_pit.sqlite3` | 6,475,485,184 | 约 6.031 GiB | 2026-08-25 00:12 | 存在 |
| `bybit_public_pit.sqlite3-wal` | — | — | — | 不存在 |
| `bybit_public_pit.sqlite3-shm` | — | — | — | 不存在 |

仅记录文件系统元数据；未打开数据库内容，未执行 SQLite checkpoint、`VACUUM`、`integrity_check`、迁移或任何数据库写入。

## 4. D 盘容量

- 已用空间：约 575.40 GB
- 剩余空间：约 61.26 GB

## 5. Git 工作区未提交变化

快照命令：`git-local.ps1 status --short`

### source_code

- 无。

### tests

- 无。

### docs

- 快照时无。
- 本停止报告按明确指定路径创建于 `D:\Money\CODEX_STOP_REPORT_20260825.md`。由于 `D:\Money` 是当前 Git 工作区根目录，该文件是在状态快照之后新增的未跟踪报告文件；未对其执行额外 Git 检查。

### runtime_reports

- `M ai_bot3/ai_bot3/model_results/evaluation/execution_cost_report.json`
- `M ai_bot3/ai_bot3/model_results/evaluation/factor_ablation_report.json`
- `M ai_bot3/ai_bot3/model_results/evaluation/lockbox_report.json`
- `M ai_bot3/ai_bot3/model_results/evaluation/profitability_report.json`
- `M ai_bot3/ai_bot3/model_results/evaluation/walk_forward_report.json`

### databases

- Git 状态快照中无数据库文件变化。

### caches

- `?? .codex-pytest/`（未跟踪的本地 Python/pytest 依赖与缓存目录；未删除、未修改、未清理）

### Diff 统计（仅已跟踪文件）

```text
5 files changed, 15757 insertions(+), 309 deletions(-)
```

上述变化均未处理、未修复、未提交、未推送、未删除。

## 6. 明确门禁状态

```text
profitability_gate=FAILED
candidate_count=0
live_count=0
mainnet=DISABLED
```

本报告不证明盈利，不证明保本，不证明 testnet 或 live 可用。

## 7. 当前已知但未完成事项

- profitability training / rebuild 已人工停止，盈利门禁仍为失败，未形成候选发布版本。
- historical backfill（包括 Bybit 历史、K 线和衍生品回填）已人工停止，完整性与后续范围未在本次核验。
- public PIT collector 已人工停止，采集完整性和恢复策略未在本次核验。
- 五个 evaluation JSON 报告仍有大量未提交变化，未在本次审查、修复或归档。
- `.codex-pytest/` 仍为未跟踪缓存/本地依赖目录，未清理。
- Draft PR #4 必须继续保持 Draft、open、unmerged；其远端状态未在本次联网核验。
- Issue #3 的持续审计与开发循环保持停止，不应由本次执行自动恢复。
- testnet 与 live 均未启用、未验证；mainnet 保持禁用。
- 未生成 `candidate_release_manifest`，未生成 `OperationTicket`。
- SQLite checkpoint 由人工在本次之前单独执行；本次未重复执行，也未做完整性检查。

## 8. 建议拆分的后续独立任务（本次不得执行，且未执行）

1. 独立的 profitability gate 复盘：冻结输入快照，审查失败原因，并事先定义退出条件。
2. 独立的 evaluation 报告审查：只审查五个未提交 JSON 的来源、可复现性与保留策略。
3. 独立的 historical backfill 计划：先确定数据缺口、磁盘预算、速率限制、断点和停止条件，再决定是否恢复。
4. 独立的 public PIT collector 完整性核验：明确时间覆盖、去重、缺口和恢复策略。
5. 独立的工作区卫生任务：决定 `.codex-pytest/` 的保留、忽略或受控清理方式。
6. 独立的 Draft PR #4 审查任务：继续保持 Draft，不合并；仅在获得明确授权后处理远端状态。
7. 独立的 testnet 安全门禁任务：在不触及 live/mainnet 的前提下定义启用条件；不得从本报告推断可用性。

## 9. 停止声明

本次没有运行训练、回测、消融、模型推理、历史回填、public PIT collector、全量 pytest 或任何数据库维护；没有访问外网或 GitHub；没有修改代码、测试、工作流、配置、PR、Issue 或分支；没有 commit、push、merge、rebase、reset、clean、切换 main 或删除任何数据。

`STOPPED_CLEANLY`
