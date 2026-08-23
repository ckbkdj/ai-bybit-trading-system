from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from core.evaluation.profitability_gate import ProfitabilityGateResult


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ProfitabilityReleaseManifest:
    schema_version: str
    release_id: str
    stage: str
    model_family: str
    model_artifact_sha256: str
    profitability_report_sha256: str
    lockbox_fingerprint: str
    code_commit: str
    created_at: str
    live_allowed: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def create_candidate_manifest(
    path: Path,
    *,
    gate: ProfitabilityGateResult,
    profitability_report_path: Path,
    model_artifact_path: Path,
    lockbox_fingerprint: str,
    code_commit: str,
) -> ProfitabilityReleaseManifest:
    if not gate.passed or gate.stage != "candidate" or gate.candidate_count != 1:
        raise ValueError("candidate manifest is forbidden when profitability gate has not passed")
    report_hash = _sha256(profitability_report_path)
    model_hash = _sha256(model_artifact_path)
    release_id = hashlib.sha256(
        f"{report_hash}|{model_hash}|{lockbox_fingerprint}|{code_commit}".encode()
    ).hexdigest()[:32]
    manifest = ProfitabilityReleaseManifest(
        schema_version="profitability-release.v1",
        release_id=f"pr_{release_id}",
        stage="candidate",
        model_family="profitability_two_stage",
        model_artifact_sha256=model_hash,
        profitability_report_sha256=report_hash,
        lockbox_fingerprint=lockbox_fingerprint,
        code_commit=code_commit,
        created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        live_allowed=False,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)
    return manifest


def verify_candidate_authorization(
    profitability_report_path: Path | None,
    manifest_path: Path | None,
) -> tuple[bool, str]:
    if profitability_report_path is None or manifest_path is None:
        return False, "profitability_report_or_manifest_missing"
    if not profitability_report_path.exists() or not manifest_path.exists():
        return False, "profitability_report_or_manifest_missing"
    try:
        report = json.loads(profitability_report_path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False, "profitability_release_json_invalid"
    if report.get("profitability_gate") != "PASSED":
        return False, "profitability_gate_failed"
    if int(report.get("candidate_count", 0)) != 1 or int(report.get("live_count", 0)) != 0:
        return False, "profitability_candidate_counts_invalid"
    if manifest.get("stage") != "candidate" or manifest.get("model_family") != "profitability_two_stage":
        return False, "profitability_manifest_stage_invalid"
    if bool(manifest.get("live_allowed")):
        return False, "profitability_manifest_must_not_enable_live"
    if manifest.get("profitability_report_sha256") != _sha256(profitability_report_path):
        return False, "profitability_report_hash_mismatch"
    return True, "verified_profitability_candidate"


__all__: Sequence[str] = (
    "ProfitabilityReleaseManifest",
    "create_candidate_manifest",
    "verify_candidate_authorization",
)
