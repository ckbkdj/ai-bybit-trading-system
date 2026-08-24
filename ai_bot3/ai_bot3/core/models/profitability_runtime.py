from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from contracts.horizons import horizon_for_mode
from core.features.profitability_technical import (
    LEGACY_BRAIN_FEATURE_COLUMNS,
    TECHNICAL_FEATURE_COLUMNS,
    engineer_profitability_features,
)
from core.features.registry import default_registry
from core.models.two_stage import TwoStageAlphaModel
from core.release.profitability_release import verify_candidate_authorization
from core.training.bybit_pit_panel import BybitPITFeatureSource
from core.training.flow_pit_panel import FLOW_FEATURE_CONTRACTS, FlowPITFeatureSource
from core.training.macro_pit_panel import MACRO_FEATURE_CONTRACTS, MacroPITFeatureSource
from core.training.pooled_panel import causal_regime_labels


EXTERNAL_FEATURE_ALIASES: Mapping[str, str] = {
    "spy_return": "cross_asset_spy_ret_1d",
    "qqq_return": "cross_asset_qqq_ret_1d",
    "tlt_return": "cross_asset_tlt_ret_1d",
    "uup_return": "cross_asset_uup_ret_1d",
    "gld_return": "cross_asset_gld_ret_1d",
    "uso_return": "cross_asset_uso_ret_1d",
    "xlv_return": "cross_asset_xlv_ret_1d",
    "ibb_return": "cross_asset_ibb_ret_1d",
    "fxi_return": "cross_asset_fxi_ret_1d",
    "kweb_return": "cross_asset_kweb_ret_1d",
    "coin_return": "cross_asset_coin_ret_1d",
    "mstr_return": "cross_asset_mstr_ret_1d",
}


def _utc(value: Any) -> datetime:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError("alpha inference timestamp is invalid")
    return parsed.to_pydatetime().astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rejected(reason: str, *, horizon_sec: int | None = None) -> dict[str, object]:
    return {
        "status": "blocked",
        "reason": reason,
        "model_family": "profitability_two_stage",
        "release_stage": "rejected",
        "profitability_gate": "FAILED",
        "horizon_sec": horizon_sec,
        "decision": "NO_TRADE",
        "actionable": False,
        "shadow_actionable": False,
        "direction": "flat",
        "lower_bound_net_edge_bps": None,
    }


def _decision_times(frame: pd.DataFrame, latest_decision_at: Any | None) -> pd.Series:
    for column in ("close_at", "close_time", "timestamp", "ts"):
        if column in frame:
            values = pd.to_datetime(frame[column], utc=True, errors="coerce")
            break
    else:
        values = pd.Series(
            pd.to_datetime(frame.index, utc=True, errors="coerce"), index=frame.index
        )
    values = pd.Series(values).reset_index(drop=True)
    if values.isna().any():
        raise ValueError("alpha inference frame has invalid decision timestamps")
    if not values.is_monotonic_increasing:
        raise ValueError("alpha inference frame must be chronological")
    if latest_decision_at is not None:
        latest = _utc(latest_decision_at)
        if latest < values.iloc[-1].to_pydatetime().astimezone(timezone.utc):
            raise ValueError("declared data cutoff precedes the last feature row")
        values.iloc[-1] = pd.Timestamp(latest)
    return values


def _external_values(
    context: Mapping[str, Any] | None,
    *,
    decision_at: datetime,
) -> tuple[dict[str, float], dict[str, object]]:
    if not isinstance(context, Mapping):
        return {}, {"status": "missing"}
    data = context.get("data")
    if not isinstance(data, Mapping):
        return {}, {"status": str(context.get("status") or "missing")}
    available_at = _utc(data.get("available_at"))
    if available_at > decision_at:
        raise ValueError("external panel was unavailable at the inference cutoff")
    if data.get("hash_verified") is not True:
        raise ValueError("external panel hash is not verified")
    raw = data.get("features")
    if not isinstance(raw, Mapping):
        raise ValueError("external panel feature payload is missing")
    values: dict[str, float] = {}
    for model_name, provider_name in EXTERNAL_FEATURE_ALIASES.items():
        if provider_name in raw:
            value = float(raw[provider_name])
            if np.isfinite(value):
                values[model_name] = value
    evidence = {
        "status": str(context.get("status") or "unknown"),
        "source": context.get("source"),
        "available_at": _iso(available_at),
        "latest_pass_run_id": data.get("latest_pass_run_id"),
        "canonical_sha256": data.get("canonical_sha_from_receipt"),
        "hash_verified": True,
    }
    return values, evidence


@lru_cache(maxsize=16)
def _verified_pit_history(
    source_kind: str,
    store_path: str,
    names: tuple[str, ...],
    maximum_sequence: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Verify immutable raw evidence once for each append-only store snapshot."""

    path = Path(store_path)
    if source_kind == "macro":
        source = MacroPITFeatureSource(path)
    elif source_kind == "flow":
        source = FlowPITFeatureSource(path)
    else:  # pragma: no cover - internal programming error
        raise ValueError(f"unsupported PIT source kind: {source_kind}")
    return source.load(names, maximum_sequence=maximum_sequence)


def _latest_global_pit_values(
    source_kind: str,
    store_path: Path,
    names: list[str],
    *,
    decision_at: datetime,
) -> tuple[dict[str, float], dict[str, object]]:
    """Resolve one strict as-of macro/flow row from a frozen verified snapshot."""

    path = Path(store_path).expanduser().resolve()
    if source_kind == "macro":
        source = MacroPITFeatureSource(path)
    elif source_kind == "flow":
        source = FlowPITFeatureSource(path)
    else:  # pragma: no cover - internal programming error
        raise ValueError(f"unsupported PIT source kind: {source_kind}")
    requested = tuple(dict.fromkeys(names))
    maximum_sequence = source.maximum_sequence()
    history, snapshot = _verified_pit_history(
        source_kind,
        str(path),
        requested,
        maximum_sequence,
    )
    joined = source.join(
        pd.DataFrame({"decision_at": [pd.Timestamp(decision_at)]}),
        names=requested,
        history=history,
    )
    values: dict[str, float] = {}
    availability: dict[str, str] = {}
    missing: list[str] = []
    for name in requested:
        value = pd.to_numeric(joined.loc[0, name], errors="coerce")
        available_at = pd.to_datetime(
            joined.loc[0, f"{name}__available_at"], utc=True, errors="coerce"
        )
        if pd.isna(value) or pd.isna(available_at):
            missing.append(name)
            continue
        values[name] = float(value)
        availability[name] = available_at.isoformat().replace("+00:00", "Z")
    if missing:
        raise ValueError(
            f"fresh {source_kind} PIT features unavailable: {sorted(missing)}"
        )
    return values, {
        "status": "verified",
        "source": snapshot.get("source"),
        "database": str(path),
        "requested_features": list(requested),
        "available_at": availability,
        "snapshot_maximum_sequence": maximum_sequence,
        "snapshot_sha256": snapshot.get("snapshot_sha256"),
        "response_count": snapshot.get("response_count"),
        "raw_response_hashes_verified": snapshot.get(
            "raw_response_hashes_verified"
        ),
    }


def build_current_feature_rows(
    frame: pd.DataFrame,
    *,
    symbol: str,
    horizon_sec: int,
    model_feature_columns: list[str],
    latest_decision_at: Any | None = None,
    external_panel_context: Mapping[str, Any] | None = None,
    bybit_pit_store_path: Path | None = None,
    macro_pit_store_path: Path | None = None,
    flow_pit_store_path: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if len(frame) < 49:
        raise ValueError("at least 49 chronological bars are required for alpha inference")
    history = engineer_profitability_features(frame.reset_index(drop=True))
    decision_times = _decision_times(frame, latest_decision_at)
    history["symbol"] = symbol.strip().upper()
    history["decision_at"] = decision_times
    history["regime"] = causal_regime_labels(history).to_numpy()
    latest = history.iloc[-1]
    decision_at = decision_times.iloc[-1].to_pydatetime().astimezone(timezone.utc)
    session = (
        "asia" if decision_at.hour < 8 else "europe" if decision_at.hour < 16 else "americas"
    )
    external, external_evidence = _external_values(
        external_panel_context, decision_at=decision_at
    )
    required_macro_features = [
        column for column in model_feature_columns if column in MACRO_FEATURE_CONTRACTS
    ]
    macro_values: dict[str, float] = {}
    macro_evidence: dict[str, object] = {"status": "not_required"}
    if required_macro_features:
        if macro_pit_store_path is None:
            raise ValueError("macro PIT store is required by the signed feature contract")
        macro_values, macro_evidence = _latest_global_pit_values(
            "macro",
            macro_pit_store_path,
            required_macro_features,
            decision_at=decision_at,
        )
    required_flow_features = [
        column for column in model_feature_columns if column in FLOW_FEATURE_CONTRACTS
    ]
    flow_values: dict[str, float] = {}
    flow_evidence: dict[str, object] = {"status": "not_required"}
    if required_flow_features:
        if flow_pit_store_path is None:
            raise ValueError("flow PIT store is required by the signed feature contract")
        flow_values, flow_evidence = _latest_global_pit_values(
            "flow",
            flow_pit_store_path,
            required_flow_features,
            decision_at=decision_at,
        )
    required_bybit_features = []
    registry = default_registry()
    for column in model_feature_columns:
        definition = registry.get(column)
        if definition and definition.factor_set in {
            "market.microstructure.v1",
            "crypto.derivatives.v1",
        }:
            required_bybit_features.append(column)
    bybit_values: dict[str, float] = {}
    bybit_evidence: dict[str, object] = {"status": "not_required"}
    if required_bybit_features:
        if bybit_pit_store_path is None:
            raise ValueError("Bybit PIT store is required by the signed feature contract")
        source = BybitPITFeatureSource(bybit_pit_store_path, registry=registry)
        bybit_values, bybit_evidence = source.latest(
            symbol,
            required_bybit_features,
            decision_at=decision_at,
        )
        missing_bybit = sorted(set(required_bybit_features).difference(bybit_values))
        if missing_bybit:
            raise ValueError(
                f"fresh symbol-specific Bybit PIT features unavailable: {missing_bybit}"
            )
        bybit_evidence["status"] = "verified"
    base: dict[str, object] = {
        "symbol": symbol.strip().upper(),
        "horizon_sec": horizon_sec,
        "liquidity": float(latest["liquidity"]),
        "volatility": float(latest["volatility"]),
        "session": session,
        "regime": str(latest["regime"]),
    }
    base.update({name: float(latest[name]) for name in TECHNICAL_FEATURE_COLUMNS})
    base.update(
        {
            name: float(latest[name])
            for name in LEGACY_BRAIN_FEATURE_COLUMNS
            if name in model_feature_columns
        }
    )
    base.update(external)
    base.update(macro_values)
    base.update(flow_values)
    base.update(bybit_values)
    missing = [
        column
        for column in model_feature_columns
        if column != "side" and (column not in base or pd.isna(base[column]))
    ]
    if missing:
        raise ValueError(f"runtime model features unavailable: {missing}")
    rows = []
    for side in ("BUY", "SELL"):
        row = dict(base)
        row["side"] = side
        rows.append(row)
    feature_payload = {
        key: value for key, value in base.items() if key in model_feature_columns
    }
    snapshot_hash = hashlib.sha256(
        json.dumps(
            {"decision_at": _iso(decision_at), "features": feature_payload},
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return pd.DataFrame(rows), {
        "decision_at": _iso(decision_at),
        "feature_snapshot_sha256": snapshot_hash,
        "external_panel": external_evidence,
        "macro_pit": macro_evidence,
        "flow_pit": flow_evidence,
        "bybit_public_pit": bybit_evidence,
    }


def generate_profitability_alpha_prediction(
    frame: pd.DataFrame,
    *,
    symbol: str,
    mode: str,
    latest_decision_at: Any | None = None,
    external_panel_context: Mapping[str, Any] | None = None,
    bybit_pit_store_path: Path | None = None,
    macro_pit_store_path: Path | None = None,
    flow_pit_store_path: Path | None = None,
    model_bundle_path: Path | None = None,
    profitability_report_path: Path | None = None,
    candidate_manifest_path: Path | None = None,
    strategy_release_id: str | None = None,
) -> dict[str, object]:
    """Run the versioned two-stage Alpha in production, fail-closed by default."""

    try:
        horizon = horizon_for_mode(mode)
    except ValueError as exc:
        return _rejected(str(exc))
    configured_bundle = model_bundle_path or (
        Path(os.environ["AI_BOT_PROFITABILITY_MODEL_BUNDLE"])
        if os.environ.get("AI_BOT_PROFITABILITY_MODEL_BUNDLE")
        else None
    )
    if configured_bundle is None:
        return _rejected("profitability_model_bundle_missing", horizon_sec=horizon)
    bundle_path = Path(configured_bundle).expanduser().resolve()
    try:
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        if bundle.get("schema_version") != "profitability-model-bundle.v2":
            raise ValueError("unsupported profitability model bundle schema")
        if bundle.get("model_family") != "profitability_two_stage":
            raise ValueError("profitability model family mismatch")
        models = bundle.get("models")
        hashes = bundle.get("model_sha256")
        if not isinstance(models, Mapping) or not isinstance(hashes, Mapping):
            raise ValueError("model paths or hashes are missing")
        relative = Path(str(models[str(horizon)]))
        if relative.is_absolute():
            raise ValueError("model path must be relative to its signed bundle")
        model_path = (bundle_path.parent / relative).resolve()
        if not model_path.is_relative_to(bundle_path.parent.resolve()):
            raise ValueError("model path escapes its signed bundle directory")
        actual_model_sha = _sha256(model_path)
        if actual_model_sha != str(hashes.get(str(horizon)) or ""):
            raise ValueError("horizon model hash mismatch")
        model = TwoStageAlphaModel.load(model_path)
        formal_contract = bundle.get("formal_feature_columns", {})
        if isinstance(formal_contract, Mapping):
            formal_features = [str(value) for value in formal_contract.get(str(horizon), [])]
        else:
            formal_features = [str(value) for value in formal_contract]
        if model.feature_columns != formal_features:
            raise ValueError("bundle and model feature contracts differ")
        rows, feature_evidence = build_current_feature_rows(
            frame,
            symbol=symbol,
            horizon_sec=horizon,
            model_feature_columns=formal_features,
            latest_decision_at=latest_decision_at,
            external_panel_context=external_panel_context,
            bybit_pit_store_path=(
                bybit_pit_store_path
                or (
                    Path(os.environ["BYBIT_PUBLIC_PIT_STORE"])
                    if os.environ.get("BYBIT_PUBLIC_PIT_STORE")
                    else None
                )
            ),
            macro_pit_store_path=(
                macro_pit_store_path
                or (
                    Path(os.environ["MACRO_PIT_STORE"])
                    if os.environ.get("MACRO_PIT_STORE")
                    else None
                )
            ),
            flow_pit_store_path=(
                flow_pit_store_path
                or (
                    Path(os.environ["FLOW_PIT_STORE"])
                    if os.environ.get("FLOW_PIT_STORE")
                    else None
                )
            ),
        )
        predictions = model.predict(rows)

        report_path = profitability_report_path or (
            Path(os.environ["AI_BOT_PROFITABILITY_REPORT"])
            if os.environ.get("AI_BOT_PROFITABILITY_REPORT")
            else None
        )
        manifest_path = candidate_manifest_path or (
            Path(os.environ["AI_BOT_CANDIDATE_RELEASE_MANIFEST"])
            if os.environ.get("AI_BOT_CANDIDATE_RELEASE_MANIFEST")
            else None
        )
        authorized, authorization_reason = verify_candidate_authorization(
            report_path, manifest_path
        )
        manifest: dict[str, object] = {}
        bundle_sha = _sha256(bundle_path)
        if authorized and manifest_path is not None:
            manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
            authorized = (
                str(manifest.get("model_artifact_sha256") or "") == bundle_sha
                and str(manifest.get("lockbox_fingerprint") or "")
                == str(bundle.get("lockbox_fingerprint") or "")
                and bundle.get("release_stage") == "candidate"
                and bundle.get("profitability_gate") == "PASSED"
            )
            if not authorized:
                authorization_reason = "candidate_bundle_or_lockbox_hash_mismatch"

        candidates = []
        for side, prediction in zip(("BUY", "SELL"), predictions):
            direction_ok = (
                side == "BUY" and prediction.p_up >= prediction.p_down
            ) or (side == "SELL" and prediction.p_down >= prediction.p_up)
            if direction_ok:
                candidates.append((prediction.decision == "TRADE", prediction, side))
        if not candidates:
            raise ValueError("direction model produced no consistent side")
        _, selected, selected_side = max(
            candidates,
            key=lambda item: (
                item[0],
                item[1].lower_bound_net_edge,
                item[1].meta_trade_probability,
            ),
        )
        shadow_trade = selected.decision == "TRADE" and selected.lower_bound_net_edge > 0
        release_stage = "candidate" if authorized else "rejected"
        strategy_id = strategy_release_id or os.environ.get(
            "AI_BOT_STRATEGY_RELEASE_ID"
        )
        return {
            "status": "ok",
            "model_family": "profitability_two_stage",
            "model_bundle_id": bundle.get("trial_id"),
            "model_artifact_sha256": bundle_sha,
            "model_file_sha256": actual_model_sha,
            "horizon_sec": horizon,
            "release_stage": release_stage,
            "profitability_gate": "PASSED" if authorized else "FAILED",
            "authorization_reason": authorization_reason,
            "release_id": manifest.get("release_id"),
            "lockbox_fingerprint": bundle.get("lockbox_fingerprint"),
            "strategy_release_id": strategy_id,
            "decision": selected.decision,
            "actionable": bool(authorized and shadow_trade),
            "shadow_actionable": bool(shadow_trade),
            "side": selected_side,
            "direction": "long" if selected_side == "BUY" else "short",
            "p_down": selected.p_down,
            "p_flat": selected.p_flat,
            "p_up": selected.p_up,
            "expected_net_return": selected.expected_net_return,
            "expected_net_return_bps": selected.expected_net_return * 10_000,
            "return_quantiles_bps": {
                "p10": selected.return_p10 * 10_000,
                "p50": selected.return_p50 * 10_000,
                "p90": selected.return_p90 * 10_000,
            },
            "expected_mae_bps": selected.expected_mae * 10_000,
            "expected_mfe_bps": selected.expected_mfe * 10_000,
            "uncertainty": selected.uncertainty,
            "meta_trade_probability": selected.meta_trade_probability,
            "lower_bound_net_edge_bps": selected.lower_bound_net_edge * 10_000,
            "feature_evidence": feature_evidence,
            "code_commit": bundle.get("code_commit"),
        }
    except Exception as exc:
        return _rejected(
            f"{type(exc).__name__}: {exc}", horizon_sec=horizon
        )


__all__ = (
    "EXTERNAL_FEATURE_ALIASES",
    "build_current_feature_rows",
    "generate_profitability_alpha_prediction",
)
