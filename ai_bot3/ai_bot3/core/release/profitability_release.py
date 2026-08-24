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
    "statistical_overfit_report.json",
    "data_coverage_report.json",
    "missing_intervals_report.json",
    "independent_timestamp_count_report.json",
    "calibration_coverage_report.json",
    "nested_cv_report.json",
    "signal_funnel_report.json",
    "intratrade_drawdown_report.json",
    "production_replay_report.json",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _evidence_semantic_failure(name: str, path: Path) -> str | None:
    if name not in {
        "data_coverage_report.json",
        "missing_intervals_report.json",
        "independent_timestamp_count_report.json",
        "calibration_coverage_report.json",
        "nested_cv_report.json",
        "signal_funnel_report.json",
        "intratrade_drawdown_report.json",
        "production_replay_report.json",
    }:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "invalid_json"
    if not isinstance(payload, Mapping):
        return "invalid_payload"
    if name == "data_coverage_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "coverage_incomplete"
        if int(payload.get("passed_series_count", -1)) != int(
            payload.get("expected_series_count", -2)
        ):
            return "coverage_series_failed"
    elif name == "missing_intervals_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "interval_audit_incomplete"
        if int(payload.get("total_discontinuity_count", -1)) != 0:
            return "discontinuities_present"
    elif name == "independent_timestamp_count_report.json":
        if payload.get("status") != "PASSED":
            return "independent_timestamp_audit_incomplete"
        if not bool(payload.get("raw_source_complete")) or not bool(
            payload.get("outer_oos_complete")
        ):
            return "independent_timestamp_scope_incomplete"
    elif name == "calibration_coverage_report.json":
        development = payload.get("development")
        lockbox = payload.get("lockbox")
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "calibration_coverage_incomplete"
        if not isinstance(development, Mapping) or not isinstance(lockbox, Mapping):
            return "calibration_scopes_missing"
        development_portfolio = development.get("portfolio")
        lockbox_portfolio = lockbox.get("portfolio")
        if not isinstance(development_portfolio, Mapping) or not bool(
            development_portfolio.get("passed")
        ):
            return "development_calibration_failed"
        if not isinstance(lockbox_portfolio, Mapping) or not bool(
            lockbox_portfolio.get("passed")
        ):
            return "lockbox_calibration_failed"
        if bool(lockbox.get("used_for_calibration_or_tuning")) or bool(
            lockbox.get("alternative_models_scored")
        ):
            return "lockbox_calibration_policy_violated"
    elif name == "nested_cv_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "nested_cv_incomplete"
        if payload.get("outer_oos_used_for_tuning") is not False:
            return "outer_oos_used_for_tuning"
    elif name == "signal_funnel_report.json":
        development = payload.get("development")
        lockbox = payload.get("lockbox")
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "signal_funnel_incomplete"
        if not isinstance(development, Mapping) or not isinstance(lockbox, Mapping):
            return "signal_funnel_scopes_missing"
        if development.get("status") != "PASSED" or lockbox.get("status") != "PASSED":
            return "zero_signal_or_trade_evidence"
        if bool(development.get("zero_signal_or_trade_result_accepted")) or bool(
            lockbox.get("zero_signal_or_trade_result_accepted")
        ):
            return "zero_signal_or_trade_policy_violated"
    elif name == "intratrade_drawdown_report.json":
        development = payload.get("development")
        lockbox = payload.get("lockbox")
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "intratrade_drawdown_incomplete"
        if not isinstance(development, Mapping) or not isinstance(lockbox, Mapping):
            return "intratrade_drawdown_scopes_missing"
        for scope, evidence in (("development", development), ("lockbox", lockbox)):
            if evidence.get("status") != "PASSED":
                return f"{scope}_intratrade_drawdown_failed"
            if not bool(evidence.get("mark_to_market_used")) or int(
                evidence.get("equity_observation_count", 0)
            ) <= 0:
                return f"{scope}_mark_to_market_incomplete"
    elif name == "production_replay_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "production_replay_incomplete"
        if bool(payload.get("lockbox_used")) or bool(
            payload.get("alternative_models_scored")
        ):
            return "production_replay_scope_violated"
        if int(payload.get("failed_sample_count", -1)) != 0 or int(
            payload.get("observed_sample_count", -1)
        ) != int(payload.get("expected_sample_count", -2)):
            return "production_replay_samples_failed"
        if payload.get("final_bundle_models_match_replayed") is not True or not str(
            payload.get("final_model_bundle_sha256") or ""
        ):
            return "production_replay_final_bundle_unbound"
    return None


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
    for name in REQUIRED_EVIDENCE_REPORTS:
        failure = _evidence_semantic_failure(name, Path(evidence_paths[name]))
        if failure is not None:
            raise ValueError(
                f"candidate manifest evidence incomplete:{name}:{failure}"
            )
    replay_payload = json.loads(
        Path(evidence_paths["production_replay_report.json"]).read_text(
            encoding="utf-8"
        )
    )
    if replay_payload.get("final_model_bundle_sha256") != model_hash:
        raise ValueError(
            "candidate manifest production replay does not bind the final model bundle"
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
        failure = _evidence_semantic_failure(name, evidence_path)
        if failure is not None:
            return False, f"profitability_evidence_incomplete:{name}:{failure}"
        if name == "production_replay_report.json":
            replay_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
            if replay_payload.get("final_model_bundle_sha256") != manifest.get(
                "model_artifact_sha256"
            ):
                return False, "profitability_production_replay_bundle_hash_mismatch"
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
