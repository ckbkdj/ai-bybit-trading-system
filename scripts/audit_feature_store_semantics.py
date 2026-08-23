from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
AI_ROOT = ROOT / "ai_bot3" / "ai_bot3"
if str(AI_ROOT) not in sys.path:
    sys.path.insert(0, str(AI_ROOT))

from core.feature_store_audit import FeatureStoreSemanticAuditor, attrition_payload  # noqa: E402
from core.kline_feature_store import KlineFeatureStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Kline feature semantic acceptance")
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument(
        "--config", type=Path, default=AI_ROOT / "config.yml"
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--recompute-groups", type=int, default=25)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8")) or {}
    store = KlineFeatureStore(args.db, cfg, read_only=True)
    semantic = FeatureStoreSemanticAuditor(store).audit(
        recompute_groups=args.recompute_groups
    )
    attrition = attrition_payload(store)
    deployment_status = "PASS"
    if semantic["status"] != "PASS" or any(
        item["feature_version_rows"] <= 0 or item["split_status"] != "ready"
        for item in attrition
    ):
        deployment_status = "BLOCKED"
    payload = {
        "deployment_status": deployment_status,
        "semantic_acceptance": semantic,
        "dataset_attrition": attrition,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if payload["deployment_status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
