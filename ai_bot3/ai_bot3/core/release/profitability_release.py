from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from core.evaluation.profitability_gate import ProfitabilityGateResult


REQUIRED_EVIDENCE_REPORTS = (
    "walk_forward_report.json",
    "lockbox_report.json",
    "factor_ablation_report.json",
    "execution_cost_report.json",
    "capital_preservation_report.json",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _release_id(
    *,
    profitability_report_sha256: str,
    model_artifact_sha256: str,
    evidence_report_sha256: Mapping[str, str],
    lockbox_fingerprint: str,
    code_commit: str,
) -> str:
    token = hashlib.sha256(
        (
            f"{profitability_report_sha256}|{model_artifact_sha256}|"
            f"{lockbox_fingerprint}|{code_commit}|"
            + json.dumps(dict(evidence_report_sha256), sort_keys=True)
        ).encode()
    ).hexdigest()[:32]
    return f"pr_{token}"


@dataclass(frozen=True)
class ProfitabilityReleaseManifest:
    schema_version: str
    release_id: str
    stage: str
    model_family: str
    model_artifact_sha256: str
    profitability_report_sha256: str
    evidence_report_sha256: Mapping[str, str]
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
    evidence_report_paths: Mapping[str, Path] | None = None,
) -> ProfitabilityReleaseManifest:
    if not gate.passed or gate.stage != "candidate" or gate.candidate_count != 1:
        raise ValueError("candidate manifest is forbidden when profitability gate has not passed")
    report_hash = _sha256(profitability_report_path)
    model_hash = _sha256(model_artifact_path)
    evidence_paths = dict(evidence_report_paths or {})
    missing = [name for name in REQUIRED_EVIDENCE_REPORTS if name not in evidence_paths]
    if missing:
        raise ValueError(
            "candidate manifest requires every evidence report: " + ", ".join(missing)
        )
    evidence_hashes = {
        name: _sha256(Path(evidence_paths[name]))
        for name in REQUIRED_EVIDENCE_REPORTS
    }
    manifest = ProfitabilityReleaseManifest(
        schema_version="profitability-release.v2",
        release_id=_release_id(
            profitability_report_sha256=report_hash,
            model_artifact_sha256=model_hash,
            evidence_report_sha256=evidence_hashes,
            lockbox_fingerprint=lockbox_fingerprint,
            code_commit=code_commit,
        ),
        stage="candidate",
        model_family="profitability_two_stage",
        model_artifact_sha256=model_hash,
        profitability_report_sha256=report_hash,
        evidence_report_sha256=evidence_hashes,
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
    if manifest.get("schema_version") != "profitability-release.v2":
        return False, "profitability_manifest_schema_invalid"
    if bool(manifest.get("live_allowed")):
        return False, "profitability_manifest_must_not_enable_live"
    if manifest.get("profitability_report_sha256") != _sha256(profitability_report_path):
        return False, "profitability_report_hash_mismatch"
    evidence_hashes = manifest.get("evidence_report_sha256")
    if not isinstance(evidence_hashes, Mapping) or any(
        name not in evidence_hashes for name in REQUIRED_EVIDENCE_REPORTS
    ):
        return False, "profitability_evidence_hashes_missing"
    evidence_root = Path(manifest_path).parent
    for name in REQUIRED_EVIDENCE_REPORTS:
        evidence_path = evidence_root / name
        if not evidence_path.is_file():
            return False, f"profitability_evidence_report_missing:{name}"
        if str(evidence_hashes[name]) != _sha256(evidence_path):
            return False, f"profitability_evidence_hash_mismatch:{name}"
    expected_release_id = _release_id(
        profitability_report_sha256=str(manifest.get("profitability_report_sha256") or ""),
        model_artifact_sha256=str(manifest.get("model_artifact_sha256") or ""),
        evidence_report_sha256={
            name: str(evidence_hashes[name]) for name in REQUIRED_EVIDENCE_REPORTS
        },
        lockbox_fingerprint=str(manifest.get("lockbox_fingerprint") or ""),
        code_commit=str(manifest.get("code_commit") or ""),
    )
    if manifest.get("release_id") != expected_release_id:
        return False, "profitability_release_id_mismatch"
    return True, "verified_profitability_candidate"


__all__: Sequence[str] = (
    "ProfitabilityReleaseManifest",
    "REQUIRED_EVIDENCE_REPORTS",
    "create_candidate_manifest",
    "verify_candidate_authorization",
)
