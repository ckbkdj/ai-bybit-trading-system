from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.brain_model import brain_stage_artifact_paths


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Controlled Brain candidate-to-live promotion")
    parser.add_argument("symbol")
    parser.add_argument("mode")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--model-dir", type=Path, default=ROOT / "models" / "brain")
    args = parser.parse_args()

    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    required = {
        "testnet_completed",
        "shadow_observation_days",
        "cost_adjusted_out_of_sample_positive",
        "max_drawdown_within_approved_limit",
        "operator_kill_switch_drill",
    }
    missing = sorted(required - set(evidence))
    if missing:
        raise SystemExit(f"evidence is missing fields: {', '.join(missing)}")
    if not all(bool(evidence[key]) for key in required if key != "shadow_observation_days"):
        raise SystemExit("promotion evidence contains a failed gate")
    if int(evidence["shadow_observation_days"]) < 30:
        raise SystemExit("at least 30 shadow observation days are required")

    cfg = {"brain_model": {"model_dir": str(args.model_dir)}}
    candidate_model, candidate_meta = brain_stage_artifact_paths(args.symbol, args.mode, "candidate", cfg)
    live_model, live_meta = brain_stage_artifact_paths(args.symbol, args.mode, "live", cfg)
    if not candidate_model.exists() or not candidate_meta.exists():
        raise SystemExit("candidate model or metadata is missing")
    metadata = json.loads(candidate_meta.read_text(encoding="utf-8"))
    if str(metadata.get("promote_decision")) != "candidate":
        raise SystemExit("only a governed candidate may be promoted")

    bundle = joblib.load(candidate_model)
    promoted_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    promotion = {
        "release_stage": "live",
        "promoted_at": promoted_at,
        "approval_id": args.approval_id,
        "evidence_sha256": _sha256(args.evidence),
        "candidate_sha256": _sha256(candidate_model),
    }
    metadata.update(promotion)
    bundle["meta"] = {**dict(bundle.get("meta") or {}), **metadata}

    temporary_model = live_model.with_suffix(".tmp.joblib")
    joblib.dump(bundle, temporary_model)
    temporary_model.replace(live_model)
    temporary_meta = live_meta.with_suffix(".tmp.json")
    temporary_meta.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary_meta.replace(live_meta)
    print(json.dumps({"live_model": str(live_model), **promotion}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
