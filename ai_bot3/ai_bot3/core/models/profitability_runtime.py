from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from contracts.horizons import MAX_CANDIDATE_KLINE_AGE_SEC, horizon_for_mode
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

MAX_EXTERNAL_PANEL_AGE = timedelta(days=7)


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
        observed = values.iloc[-1].to_pydatetime().astimezone(timezone.utc)
        if latest != observed:
            raise ValueError(
                "declared data cutoff must equal the last observed price timestamp; "
                "runtime cannot re-date stale price data"
            )
    return values


def price_frame_cutoff(frame: pd.DataFrame) -> datetime:
    """Return the same final observed timestamp used by Alpha validation."""

    values = _decision_times(frame, None)
    if values.empty:
        raise ValueError("alpha inference frame has no decision timestamps")
    return values.iloc[-1].to_pydatetime().astimezone(timezone.utc)


def _validate_price_frame(
    frame: pd.DataFrame,
    *,
    horizon_sec: int,
    latest_decision_at: Any | None,
) -> tuple[pd.Series, dict[str, object]]:
    """Recheck the price contract at the model boundary, not only in fetchers."""

    required = ("open", "high", "low", "close", "volume")
    missing = [column for column in required if column not in frame]
    if missing:
        raise ValueError(f"alpha price frame is missing OHLCV columns: {missing}")
    decision_times = _decision_times(frame, latest_decision_at)
    gaps = decision_times.diff().dropna().dt.total_seconds()
    if gaps.empty or not np.isclose(gaps.to_numpy(float), float(horizon_sec), atol=1e-6).all():
        raise ValueError("alpha price frame is discontinuous or off the signed horizon grid")
    numeric = frame.loc[:, required].apply(pd.to_numeric, errors="coerce")
    values = numeric.to_numpy(float)
    if not np.isfinite(values).all():
        raise ValueError("alpha price frame contains non-finite OHLCV values")
    if (
        (numeric[["open", "high", "low", "close"]] <= 0).any().any()
        or (numeric["volume"] < 0).any()
        or (numeric["high"] < numeric[["open", "close"]].max(axis=1)).any()
        or (numeric["low"] > numeric[["open", "close"]].min(axis=1)).any()
        or (numeric["high"] < numeric["low"]).any()
    ):
        raise ValueError("alpha price frame violates the OHLCV market-data contract")
    return decision_times, {
        "continuous": True,
        "interval_sec": int(horizon_sec),
        "observed_bar_count": len(frame),
        "first_observed_at": _iso(
            decision_times.iloc[0].to_pydatetime().astimezone(timezone.utc)
        ),
        "last_observed_at": _iso(
            decision_times.iloc[-1].to_pydatetime().astimezone(timezone.utc)
        ),
        "last_price": float(numeric["close"].iloc[-1]),
        "ohlcv_contract_valid": True,
    }


def _external_values(
    context: Mapping[str, Any] | None,
    *,
    decision_at: datetime,
) -> tuple[dict[str, float], dict[str, object]]:
    if not isinstance(context, Mapping):
        return {}, {"status": "missing"}
    status = str(context.get("status") or "missing").strip().lower()
    data = context.get("data")
    if not isinstance(data, Mapping):
        return {}, {"status": status}
    if status != "ok":
        raise ValueError(f"external panel provider status is not healthy: {status}")
    available_at = _utc(data.get("available_at"))
    if available_at > decision_at:
        raise ValueError("external panel was unavailable at the inference cutoff")
    age = decision_at - available_at
    if age > MAX_EXTERNAL_PANEL_AGE:
        raise ValueError("external panel is stale at the inference cutoff")
    if data.get("hash_verified") is not True:
        raise ValueError("external panel hash is not verified")
    revision_control = data.get("revision_control")
    if not isinstance(revision_control, Mapping):
        raise ValueError("external panel revision-control evidence is missing")
    if revision_control.get("receipt_predecessor_hash_verified") is not True:
        raise ValueError("external panel predecessor hash is not verified")
    if revision_control.get("append_only_revision_verified") is not True:
        raise ValueError("external panel history is not append-only verified")
    if revision_control.get("scoped_base_price_audit_status") != "PASS":
        raise ValueError("external base-price quality audit did not pass")
    if revision_control.get("external_derived_columns_trusted") is not False:
        raise ValueError("external panel derived-column exclusion is not proven")
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
        "status": status,
        "source": context.get("source"),
        "available_at": _iso(available_at),
        "latest_pass_run_id": data.get("latest_pass_run_id"),
        "canonical_sha256": data.get("canonical_sha_from_receipt"),
        "hash_verified": True,
        "baseline_sha256": revision_control.get("baseline_sha256"),
        "receipt_predecessor_hash_verified": True,
        "append_only_revision_verified": True,
        "scoped_base_price_audit_status": "PASS",
        "age_seconds": age.total_seconds(),
        "maximum_age_seconds": MAX_EXTERNAL_PANEL_AGE.total_seconds(),
    }
    return values, evidence


@lru_cache(maxsize=16)
def _verified_pit_history(
    source_kind: str,
    store_path: str,
    names: tuple[str, ...],
    maximum_sequence: int,
    maximum_invalidation_rowid: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Verify immutable raw evidence once for each append-only store snapshot."""

    path = Path(store_path)
    if source_kind == "macro":
        source = MacroPITFeatureSource(path)
    elif source_kind == "flow":
        source = FlowPITFeatureSource(path)
    else:  # pragma: no cover - internal programming error
        raise ValueError(f"unsupported PIT source kind: {source_kind}")
    if source_kind == "flow":
        return source.load(
            names,
            maximum_sequence=maximum_sequence,
            maximum_invalidation_rowid=maximum_invalidation_rowid,
        )
    return source.load(names, maximum_sequence=maximum_sequence)


def _latest_global_pit_values(
    source_kind: str,
    store_path: Path,
    names: list[str],
    *,
    decision_at: datetime,
    maximum_sequence: int | None = None,
    maximum_invalidation_rowid: int | None = None,
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
    if source_kind == "flow":
        current_sequence, current_invalidation_rowid = source.snapshot_watermarks()
        frozen_sequence = (
            current_sequence if maximum_sequence is None else int(maximum_sequence)
        )
        frozen_invalidation_rowid = (
            current_invalidation_rowid
            if maximum_invalidation_rowid is None
            else int(maximum_invalidation_rowid)
        )
    else:
        current_sequence = source.maximum_sequence()
        frozen_sequence = (
            current_sequence if maximum_sequence is None else int(maximum_sequence)
        )
        frozen_invalidation_rowid = 0
    history, snapshot = _verified_pit_history(
        source_kind,
        str(path),
        requested,
        frozen_sequence,
        frozen_invalidation_rowid,
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
        "snapshot_maximum_sequence": frozen_sequence,
        "snapshot_maximum_invalidation_rowid": snapshot.get(
            "snapshot_maximum_invalidation_rowid"
        ),
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
    pit_snapshot_watermarks: Mapping[str, Mapping[str, int | None]] | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if len(frame) < 49:
        raise ValueError("at least 49 chronological bars are required for alpha inference")
    decision_times, price_frame_evidence = _validate_price_frame(
        frame,
        horizon_sec=horizon_sec,
        latest_decision_at=latest_decision_at,
    )
    history = engineer_profitability_features(frame.reset_index(drop=True))
    history["symbol"] = symbol.strip().upper()
    history["decision_at"] = decision_times
    history["regime"] = causal_regime_labels(history).to_numpy()
    latest = history.iloc[-1]
    decision_at = decision_times.iloc[-1].to_pydatetime().astimezone(timezone.utc)
    session = (
        "asia" if decision_at.hour < 8 else "europe" if decision_at.hour < 16 else "americas"
    )
    required_external_features = [
        column for column in model_feature_columns if column in EXTERNAL_FEATURE_ALIASES
    ]
    external: dict[str, float] = {}
    external_evidence: dict[str, object] = {"status": "not_required"}
    if required_external_features:
        external, external_evidence = _external_values(
            external_panel_context, decision_at=decision_at
        )
        missing_external = sorted(set(required_external_features).difference(external))
        if missing_external:
            raise ValueError(
                f"fresh external panel features unavailable: {missing_external}"
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
            maximum_sequence=(
                (pit_snapshot_watermarks or {}).get("macro", {}).get(
                    "maximum_sequence"
                )
            ),
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
            maximum_sequence=(
                (pit_snapshot_watermarks or {}).get("flow", {}).get(
                    "maximum_sequence"
                )
            ),
            maximum_invalidation_rowid=(
                (pit_snapshot_watermarks or {}).get("flow", {}).get(
                    "maximum_invalidation_rowid"
                )
            ),
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
            maximum_sequence=(
                (pit_snapshot_watermarks or {}).get("bybit", {}).get(
                    "maximum_sequence"
                )
            ),
            maximum_invalidation_rowid=(
                (pit_snapshot_watermarks or {}).get("bybit", {}).get(
                    "maximum_invalidation_rowid"
                )
            ),
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
        "price_frame": price_frame_evidence,
        "external_panel": external_evidence,
        "macro_pit": macro_evidence,
        "flow_pit": flow_evidence,
        "bybit_public_pit": bybit_evidence,
    }


def select_directional_prediction(
    predictions: Sequence[object],
) -> tuple[str, object]:
    if len(predictions) != 2:
        raise ValueError("runtime expects paired BUY and SELL predictions")
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
    return selected_side, selected


def generate_profitability_alpha_prediction(
    frame: pd.DataFrame,
    *,
    symbol: str,
    mode: str,
    input_price_source: str | None = None,
    latest_decision_at: Any | None = None,
    external_panel_context: Mapping[str, Any] | None = None,
    bybit_pit_store_path: Path | None = None,
    macro_pit_store_path: Path | None = None,
    flow_pit_store_path: Path | None = None,
    model_bundle_path: Path | None = None,
    profitability_report_path: Path | None = None,
    candidate_manifest_path: Path | None = None,
    strategy_release_id: str | None = None,
    pit_snapshot_watermarks: Mapping[str, Mapping[str, int | None]] | None = None,
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
        bundle_kline_source = str(bundle.get("kline_source") or "legacy_unspecified")
        runtime_price_source = str(
            input_price_source
            or getattr(frame, "attrs", {}).get("data_source")
            or "missing"
        )
        models = bundle.get("models")
        hashes = bundle.get("model_sha256")
        if not isinstance(models, Mapping) or not isinstance(hashes, Mapping):
            raise ValueError("model paths or hashes are missing")
        if bundle.get("release_stage") == "candidate":
            approved_raw = bundle.get("approved_horizons")
            if not isinstance(approved_raw, list) or not approved_raw:
                raise ValueError("candidate bundle has no approved horizons")
            approved_horizons = {int(value) for value in approved_raw}
            if set(models) != {str(value) for value in approved_horizons}:
                raise ValueError(
                    "candidate bundle model set differs from approved horizons"
                )
            if horizon not in approved_horizons:
                raise ValueError(
                    "requested horizon is not approved by profitability evidence"
                )
            if bundle_kline_source != "bybit":
                raise ValueError(
                    "candidate bundle lacks a signed Bybit same-venue price source"
                )
        if bundle_kline_source == "bybit" and runtime_price_source != (
            "bybit_linear_last_trade_kline"
        ):
            raise ValueError(
                "Bybit-trained Alpha requires a fresh Bybit last-trade kline frame"
            )
        candidate_age_seconds: float | None = None
        candidate_maximum_age_seconds: float | None = None
        if bundle.get("release_stage") == "candidate":
            candidate_times = _decision_times(frame, latest_decision_at)
            candidate_last = candidate_times.iloc[-1].to_pydatetime().astimezone(
                timezone.utc
            )
            candidate_age = datetime.now(timezone.utc) - candidate_last
            maximum_age = timedelta(
                seconds=MAX_CANDIDATE_KLINE_AGE_SEC[horizon]
            )
            candidate_age_seconds = candidate_age.total_seconds()
            candidate_maximum_age_seconds = maximum_age.total_seconds()
            if candidate_age < timedelta(0) or candidate_age > maximum_age:
                raise ValueError(
                    "candidate price frame is stale or future-dated at the Alpha boundary"
                )
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
            pit_snapshot_watermarks=pit_snapshot_watermarks,
        )
        feature_evidence["price_path"] = {
            "status": "verified",
            "training_kline_source": bundle_kline_source,
            "runtime_price_source": runtime_price_source,
            "same_venue": bool(
                bundle_kline_source == "bybit"
                and runtime_price_source == "bybit_linear_last_trade_kline"
            ),
            "candidate_freshness_verified": bool(
                bundle.get("release_stage") == "candidate"
                and candidate_age_seconds is not None
                and candidate_maximum_age_seconds is not None
                and 0.0 <= candidate_age_seconds <= candidate_maximum_age_seconds
            ),
            "age_seconds": candidate_age_seconds,
            "maximum_age_seconds": candidate_maximum_age_seconds,
            **dict(feature_evidence["price_frame"]),
        }
        feature_evidence["runtime_contract_verified"] = True
        range_guard = model.feature_range_guard(rows)
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

        selected_side, selected = select_directional_prediction(predictions)
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
            "range_guard_score": range_guard.score,
            "range_guard_details": {
                "method": range_guard.method,
                "violation_fraction": range_guard.violation_fraction,
                "maximum_excess": range_guard.maximum_excess,
            },
            "market_regime": str(rows.iloc[-1]["regime"]),
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
    "MAX_EXTERNAL_PANEL_AGE",
    "build_current_feature_rows",
    "generate_profitability_alpha_prediction",
    "price_frame_cutoff",
    "select_directional_prediction",
)
