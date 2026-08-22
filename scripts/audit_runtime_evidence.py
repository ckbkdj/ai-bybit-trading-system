"""Stream large runtime logs and emit a redacted, reproducible incident summary.

The source logs are treated as evidence: this program never modifies them and never
copies arbitrary log text into the report.  Only known categories and normalized,
redacted templates are persisted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


TIMESTAMP = re.compile(r"(?P<ts>20\d\d-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:,\d{3})?)")
LEVEL = re.compile(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]|\b(DEBUG|INFO|WARNING|ERROR|CRITICAL):")

# These categories are deliberately specific.  Broad searches such as ``451`` or
# ``error`` create serious false positives from milliseconds, TCP ports and normal
# prose.
CATEGORIES: dict[str, re.Pattern[str]] = {
    "python_traceback": re.compile(r"^Traceback \(most recent call last\):"),
    "resource_tracker_restart": re.compile(r"resource_tracker: process died unexpectedly", re.I),
    "resource_tracker_key_error": re.compile(r"KeyError: ['\"]/(?:loky|psm)-", re.I),
    "broken_process_pool": re.compile(r"BrokenProcessPool", re.I),
    "memory_error": re.compile(r"\bMemoryError\b|out of memory|cannot allocate memory", re.I),
    "forced_process_kill": re.compile(r"强制杀死进程|SIGKILL|killed process", re.I),
    "worker_timeout": re.compile(r"(?:worker|子进程|任务|inference|训练).{0,80}(?:timeout|超时)", re.I),
    "network_timeout": re.compile(r"(?:Read timed out|ConnectTimeout|connection timed out|网络抓取失败)", re.I),
    "http_451": re.compile(r"(?:HTTP[/ ]\S+\s+451\b|status(?:_code)?[=: ]+451\b|\b451 Unavailable For Legal Reasons\b)", re.I),
    "sqlite_locked": re.compile(r"database is locked", re.I),
    "sqlite_corruption": re.compile(r"database disk image is malformed|database corruption|disk I/O error", re.I),
    "file_descriptor_exhaustion": re.compile(r"too many open files|EMFILE", re.I),
    "cuda_unavailable": re.compile(r"Could not find cuda drivers|未检测到可用 GPU", re.I),
    "tensorflow_reinitialization": re.compile(r"oneDNN custom operations are on|InitializeLog", re.I),
    "prediction_saved": re.compile(r"预测结果已保存|已保存 .+ 预测结果", re.I),
    "prediction_failed": re.compile(r"预测(?:任务)?失败|预测异常|inference failed", re.I),
    "training_completed": re.compile(r"训练完成|完成任务: TaskKind\.TRAIN", re.I),
    "training_failed": re.compile(r"训练失败|训练异常", re.I),
    "training_skipped_new_rows": re.compile(r"跳过训练: skipped_new_rows_below_threshold", re.I),
    "feature_frame_fragmented": re.compile(r"DataFrame is highly fragmented", re.I),
    "aux_source_fallback": re.compile(r"fallback 刷新|本地.+合成 fallback", re.I),
    "aux_source_success": re.compile(r"指标 .+ 已通过 /api/.+ 更新", re.I),
    "api_2xx": re.compile(r'"(?:GET|POST|PUT|DELETE|PATCH) [^\"]+ HTTP/\d(?:\.\d)?" 2\d\d\b'),
    "api_4xx": re.compile(r'"(?:GET|POST|PUT|DELETE|PATCH) [^\"]+ HTTP/\d(?:\.\d)?" 4\d\d\b'),
    "api_5xx": re.compile(r'"(?:GET|POST|PUT|DELETE|PATCH) [^\"]+ HTTP/\d(?:\.\d)?" 5\d\d\b'),
    "git_revision_missing": re.compile(r"fatal: bad revision 'HEAD'", re.I),
}

TEMPLATE_INTEREST = re.compile(
    r"\b(?:WARNING|ERROR|CRITICAL)\b|Traceback|Exception|Error:|Warning:|失败|异常|超时|跳过|fallback|强制杀死",
    re.I,
)

SECRET_VALUE = re.compile(
    r"(?i)(api[_-]?key|api[_-]?secret|secret|token|authorization|bearer|password|passwd)"
    r"(\s*[:=]\s*)([^\s,;\]}]+)"
)
URL_QUERY = re.compile(r"(https?://[^?\s]+)\?\S+", re.I)
IP_PORT = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b")
PATH = re.compile(r"(?:[A-Za-z]:\\|/)(?:[^\s:'\"]+[\\/])+[^\s:'\"]+")
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f-]{27,}\b", re.I)
FLOAT = re.compile(r"(?<![A-Za-z])[-+]?\d+\.\d+(?![A-Za-z])")
INTEGER = re.compile(r"(?<![A-Za-z])\d{2,}(?![A-Za-z])")
SPACE = re.compile(r"\s+")


def normalized_template(line: str) -> str:
    """Return a bounded redacted template, never an arbitrary raw log line."""

    value = SECRET_VALUE.sub(lambda match: f"{match.group(1)}{match.group(2)}<redacted>", line)
    value = URL_QUERY.sub(r"\1?<query>", value)
    value = TIMESTAMP.sub("<timestamp>", value)
    value = IP_PORT.sub("<ip:port>", value)
    value = UUID.sub("<uuid>", value)
    value = PATH.sub("<path>", value)
    value = FLOAT.sub("<float>", value)
    value = INTEGER.sub("<int>", value)
    value = SPACE.sub(" ", value).strip()
    return value[:360]


@dataclass
class LogAudit:
    path: str
    size_bytes: int
    sha256: str = ""
    total_lines: int = 0
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    levels: Counter[str] = field(default_factory=Counter)
    categories: Counter[str] = field(default_factory=Counter)
    top_incident_templates: list[dict[str, object]] = field(default_factory=list)


def iter_lines_and_hash(path: Path) -> tuple[Iterable[str], "hashlib._Hash"]:
    digest = hashlib.sha256()

    def lines() -> Iterable[str]:
        with path.open("rb") as handle:
            for raw in handle:
                digest.update(raw)
                yield raw.decode("utf-8", errors="replace").rstrip("\r\n")

    return lines(), digest


def audit_log(path: Path, *, top_n: int) -> LogAudit:
    result = LogAudit(path=path.as_posix(), size_bytes=path.stat().st_size)
    templates: Counter[str] = Counter()
    lines, digest = iter_lines_and_hash(path)
    for line in lines:
        result.total_lines += 1
        ts_match = TIMESTAMP.search(line)
        if ts_match:
            timestamp = ts_match.group("ts").replace(",", ".")
            if result.first_timestamp is None:
                result.first_timestamp = timestamp
            result.last_timestamp = timestamp
        level_match = LEVEL.search(line)
        if level_match:
            result.levels[level_match.group(1) or level_match.group(2)] += 1
        matched_category = False
        for name, pattern in CATEGORIES.items():
            if pattern.search(line):
                result.categories[name] += 1
                matched_category = True
        if matched_category or TEMPLATE_INTEREST.search(line):
            template = normalized_template(line)
            if template:
                templates[template] += 1
    result.sha256 = digest.hexdigest()
    result.top_incident_templates = [
        {"count": count, "template": template}
        for template, count in templates.most_common(top_n)
    ]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--top", type=int, default=25)
    args = parser.parse_args()

    missing = [str(path) for path in args.logs if not path.is_file()]
    if missing:
        parser.error(f"missing log files: {missing}")

    audits = [audit_log(path, top_n=max(1, args.top)) for path in args.logs]
    payload = {
        "schema_version": "runtime-log-audit.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "read-only streaming scan; exact category patterns; redacted normalized templates",
        "known_limitations": [
            "A repeated line is an occurrence, not necessarily an independent incident.",
            "Logs without timestamps cannot establish an incident date.",
            "Successful HTTP responses do not prove forecast correctness or data freshness.",
        ],
        "logs": [
            {
                **asdict(audit),
                "levels": dict(audit.levels),
                "categories": dict(audit.categories),
            }
            for audit in audits
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
