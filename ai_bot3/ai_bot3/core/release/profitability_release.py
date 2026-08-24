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


def _safe_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _safe_float(value: object, default: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if result == result and abs(result) != float("inf") else default


def _evidence_semantic_failure(name: str, path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "invalid_json"
    if not isinstance(payload, Mapping):
        return "invalid_payload"
    required_horizons = {"180", "900", "7200", "14400", "86400"}
    if name == "walk_forward_report.json":
        folds = payload.get("folds")
        release_datasets = payload.get("direct_execution_release_datasets")
        eligible_horizons = {
            str(value)
            for value in list(payload.get("development_eligible_horizons") or [])
        }
        if payload.get("outer_oos_used_for_tuning") is not False:
            return "outer_oos_used_for_tuning"
        if not isinstance(folds, list) or not folds:
            return "walk_forward_folds_missing"
        if not eligible_horizons or not eligible_horizons.issubset(required_horizons):
            return "precommitted_horizons_incomplete"
        if not isinstance(release_datasets, Mapping) or not eligible_horizons.issubset(
            set(map(str, release_datasets))
        ):
            return "direct_execution_release_datasets_incomplete"
        if any(
            not isinstance(release_datasets[horizon], Mapping)
            or not bool(release_datasets[horizon].get("release_walk_forward_ready"))
            for horizon in eligible_horizons
        ):
            return "direct_execution_release_dataset_failed"
        if _safe_float(payload.get("positive_fold_ratio"), 0.0) < 0.60:
            return "positive_fold_ratio_failed"
    elif name == "lockbox_report.json":
        result = payload.get("result")
        horizon_results = payload.get("horizon_results")
        eligible_horizons = {
            str(value)
            for value in list(payload.get("development_eligible_horizons") or [])
        }
        if payload.get("status") != "EVALUATED_ONCE":
            return "lockbox_not_evaluated_once"
        if payload.get("used_for_parameter_selection") is not False:
            return "lockbox_used_for_parameter_selection"
        if payload.get("lockbox_labels_materialized") is not True:
            return "lockbox_labels_not_materialized"
        if not str(payload.get("lockbox_fingerprint") or ""):
            return "lockbox_fingerprint_missing"
        if not isinstance(result, Mapping) or len(list(result.get("trades") or [])) < 100:
            return "lockbox_trade_evidence_incomplete"
        if not eligible_horizons or not eligible_horizons.issubset(required_horizons):
            return "lockbox_eligible_horizons_invalid"
        if not isinstance(horizon_results, Mapping) or set(
            map(str, horizon_results)
        ) != eligible_horizons:
            return "lockbox_horizon_evidence_incomplete"
    elif name == "factor_ablation_report.json":
        groups = payload.get("groups")
        required_groups = {
            "legacy_brain_technical",
            "bybit_orderbook",
            "public_trades",
            "basis_funding_oi",
            "liquidations",
            "execution_quality",
            "us_risk",
            "rates_usd",
            "commodities",
            "healthcare",
            "china",
            "crypto_equities",
            "stablecoin_flows",
            "fund_flows",
            "macro_vintage",
            "tier_a_events",
        }
        if payload.get("all_required_groups_evaluated") is not True:
            return "required_factor_ablation_incomplete"
        if not isinstance(groups, list) or not groups:
            return "factor_groups_missing"
        if {
            str(group.get("factor_group"))
            for group in groups
            if isinstance(group, Mapping)
        } != required_groups:
            return "required_factor_groups_incomplete"
        for group in groups:
            if not isinstance(group, Mapping):
                return "factor_group_invalid"
            horizon_results = group.get("horizon_results")
            applicable = {
                str(value) for value in list(group.get("applicable_horizons") or [])
            }
            if (
                group.get("oos_ablation_status") != "EVALUATED_OOS"
                or group.get("all_applicable_horizons_evaluated") is not True
                or not applicable
                or not isinstance(horizon_results, Mapping)
                or set(map(str, horizon_results)) != applicable
                or any(
                    not isinstance(item, Mapping)
                    or item.get("oos_ablation_status") != "EVALUATED_OOS"
                    for item in horizon_results.values()
                )
            ):
                return "factor_horizon_ablation_incomplete"
    elif name == "execution_cost_report.json":
        execution = payload.get("execution_evidence")
        normal = payload.get("normal_cost")
        stressed = payload.get("two_x_cost")
        if payload.get("evaluation_scope") != "lockbox":
            return "execution_scope_not_lockbox"
        if payload.get("execution_evidence_complete") is not True or payload.get(
            "candidate_backtest_execution_evidence_complete"
        ) is not True:
            return "candidate_execution_evidence_incomplete"
        if not isinstance(execution, Mapping) or not all(
            bool(execution.get(field))
            for field in (
                "official_pit_cost_inputs_complete",
                "simulation_complete",
                "risk_policy_compliant",
                "candidate_backtest_execution_evidence_complete",
            )
        ):
            return "direct_execution_contract_incomplete"
        if _safe_int(execution.get("proxy_execution_cost_trade_count"), -1) != 0:
            return "proxy_execution_cost_present"
        if _safe_int(execution.get("direct_execution_cost_trade_count"), 0) < 100:
            return "direct_execution_trade_count_incomplete"
        if not isinstance(normal, Mapping) or not bool(normal.get("mark_to_market_used")):
            return "normal_cost_mark_to_market_incomplete"
        if not isinstance(stressed, Mapping) or _safe_float(
            stressed.get("net_return"), -1.0
        ) < 0.0:
            return "two_x_cost_stress_failed"
    elif name == "capital_preservation_report.json":
        policy = payload.get("policy")
        if not isinstance(policy, Mapping) or payload.get("fail_closed") is not True:
            return "capital_preservation_policy_incomplete"
        exact_limits = {
            "risk_per_trade": 0.0025,
            "daily_loss_limit": 0.005,
            "weekly_loss_limit": 0.015,
            "equity_drawdown_limit": 0.03,
            "leverage_cap": 2.0,
        }
        if any(
            _safe_float(policy.get(key), -1.0) != value
            for key, value in exact_limits.items()
        ):
            return "capital_preservation_limits_changed"
        if not all(
            payload.get(field) is True
            for field in (
                "no_averaging_down",
                "no_martingale",
                "no_trade_without_stop",
                "no_trade_when_lower_bound_net_edge_lte_zero",
            )
        ):
            return "capital_preservation_prohibitions_incomplete"
    elif name == "statistical_overfit_report.json":
        development = payload.get("development")
        lockbox = payload.get("lockbox")
        eligible_horizons = {
            str(value)
            for value in list(payload.get("development_eligible_horizons") or [])
        }
        if not isinstance(development, Mapping) or not isinstance(lockbox, Mapping):
            return "statistical_scopes_missing"
        if lockbox.get("alternative_variants_scored_on_lockbox") is not False:
            return "alternative_variants_scored_on_lockbox"
        if not eligible_horizons or not eligible_horizons.issubset(required_horizons):
            return "statistical_eligible_horizons_invalid"
        for scope_name, scope in (("development", development), ("lockbox", lockbox)):
            portfolio = scope.get("portfolio")
            horizons = scope.get("horizons")
            if not isinstance(portfolio, Mapping) or portfolio.get("complete") is not True:
                return f"{scope_name}_statistical_portfolio_incomplete"
            if _safe_float(portfolio.get("deflated_sharpe_probability"), 0.0) < 0.95:
                return f"{scope_name}_deflated_sharpe_failed"
            if _safe_float(
                portfolio.get("probability_of_backtest_overfitting"), 1.0
            ) > 0.05:
                return f"{scope_name}_pbo_failed"
            horizon_keys = set(map(str, horizons)) if isinstance(horizons, Mapping) else set()
            if not eligible_horizons.issubset(horizon_keys) or (
                scope_name == "lockbox" and horizon_keys != eligible_horizons
            ):
                return f"{scope_name}_statistical_horizons_incomplete"
            if any(
                not isinstance(horizons[horizon], Mapping)
                or horizons[horizon].get("complete") is not True
                or _safe_float(
                    horizons[horizon].get("deflated_sharpe_probability"), 0.0
                )
                < 0.95
                or _safe_float(
                    horizons[horizon].get("probability_of_backtest_overfitting"),
                    1.0,
                )
                > 0.05
                for horizon in eligible_horizons
            ):
                return f"{scope_name}_statistical_horizon_failed"
    elif name == "data_coverage_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "coverage_incomplete"
        if _safe_int(payload.get("passed_series_count"), -1) != _safe_int(
            payload.get("expected_series_count"), -2
        ):
            return "coverage_series_failed"
    elif name == "missing_intervals_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "interval_audit_incomplete"
        if _safe_int(payload.get("total_discontinuity_count"), -1) != 0:
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
            if not bool(evidence.get("mark_to_market_used")) or _safe_int(
                evidence.get("equity_observation_count"), 0
            ) <= 0:
                return f"{scope}_mark_to_market_incomplete"
    elif name == "production_replay_report.json":
        if payload.get("status") != "PASSED" or not bool(payload.get("complete")):
            return "production_replay_incomplete"
        if bool(payload.get("lockbox_used")) or bool(
            payload.get("alternative_models_scored")
        ):
            return "production_replay_scope_violated"
        if _safe_int(payload.get("failed_sample_count"), -1) != 0 or _safe_int(
            payload.get("observed_sample_count"), -1
        ) != _safe_int(payload.get("expected_sample_count"), -2):
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
    if _safe_int(report.get("candidate_count"), 0) != 1 or _safe_int(
        report.get("live_count"), 0
    ) != 0:
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
