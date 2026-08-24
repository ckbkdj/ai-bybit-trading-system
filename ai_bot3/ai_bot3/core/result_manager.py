# 这个类负责保存每个模型的独立预测结果，并能聚合所有最新结果
import json
import logging
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import asyncio

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from contracts.horizons import MAX_CANDIDATE_KLINE_AGE_SEC, horizon_for_mode
from core.control_plane import ControlPlaneRepository
from core.decision.ticket_builder import TicketBuilder
from core.release.profitability_release import verify_candidate_authorization


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


# 预测结果中需要保留的扩展字段（向后兼容；缺失时回退默认值，不会导致前端崩溃）
_OPTIONAL_PREDICTION_FIELDS = (
    "raw_trend",
    "calibrated_trend",
    "raw_predicted_return",
    "predicted_return",
    "price_predicted_return",
    "price_trend",
    "calibrated_predicted_return",
    "calibrated_return",
    "calibrated_direction",
    "trade_trend_display",
    "brain_trend",
    "direction_confidence",
    "factor_bias",
    "llm_signal",
    "ensemble_score",
    "confidence",
    "fused_weights",
    "weights",
    "news_weight_total",
    "context_completeness",
    "online_learning",
    "openai_prediction",
    "data_sources_generated_at",
    "training_metadata",
    "news_training_summary",
    "validation_direction_acc",
    "validation_sign_acc",
    "validation_rmse_return",
    "walk_forward_objective",
    "selected_window",
    "selected_horizon",
    "selected_params",
    "model_version",
    "kline_last_price",
    "current_price",
    "current_price_source",
    "current_price_mtime",
    "current_price_age_seconds",
    "current_price_warning",
    "brain_prediction",
    "alpha_prediction",
    "trade_direction",
    "trade_actionable",
    "target_leverage",
    "target_raw_return",
    "expected_leveraged_return",
    "brain_training",
    "calibration_status",
    "data_source_reliable",
    "out_of_distribution_score",
    "out_of_distribution_details",
    "range_guard_score",
    "range_guard_details",
    "factor_scores",
)


def _normalize_prediction(data: Dict[str, Any], *, saving: bool = False) -> Dict[str, Any]:
    """补齐扩展字段；保留旧字段含义。"""
    if not isinstance(data, dict):
        return {"error": "invalid prediction payload"}
    out = dict(data)
    out.setdefault("generated_at", _now_iso())
    if saving:
        saved_at = _now_iso()
        out["saved_at"] = saved_at
        out["updated_at"] = saved_at
    else:
        # Reading an old file must never make it look freshly predicted.
        out.setdefault("saved_at", out.get("generated_at"))
        out.setdefault("updated_at", out.get("saved_at"))
    # raw_trend/calibrated_trend 默认与 trend 对齐
    if "raw_trend" not in out:
        out["raw_trend"] = out.get("trend")
    if "calibrated_trend" not in out:
        out["calibrated_trend"] = out.get("trend")
    if "raw_predicted_return" not in out and "predicted_return" in out:
        out["raw_predicted_return"] = out["predicted_return"]
    for k in _OPTIONAL_PREDICTION_FIELDS:
        out.setdefault(k, None)
    return out


def _atomic_json_write(path: Path, payload: Dict[str, Any]) -> None:
    """Write one complete JSON document, then atomically replace the visible file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary_name).replace(path)
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


class ResultManager:
    def __init__(
        self,
        results_dir: Path,
        *,
        control_plane_db: Path | None = None,
        tickets_enabled: bool | None = None,
        required_brain_release_stage: str | None = None,
        strategy_release_bundle=None,
        strategy_release_bundle_path: Path | None = None,
        profitability_report_path: Path | None = None,
        candidate_release_manifest_path: Path | None = None,
    ):
        self.results_dir = results_dir
        self.results_dir.mkdir(parents=True, exist_ok=True)
        if tickets_enabled is None:
            tickets_enabled = os.environ.get("AI_BOT_TICKETS_ENABLED", "true").strip().lower() in {
                "1", "true", "yes", "on"
            }
        self.tickets_enabled = bool(tickets_enabled)
        self.required_brain_release_stage = str(
            required_brain_release_stage
            or os.environ.get("AI_BOT_REQUIRED_BRAIN_RELEASE_STAGE", "live")
        ).strip().lower()
        if self.required_brain_release_stage not in {"candidate", "live"}:
            raise ValueError("required_brain_release_stage must be candidate or live")
        default_db = self.results_dir.parent / "data" / "control_plane.sqlite3"
        self.control_plane = ControlPlaneRepository(control_plane_db or default_db)
        self.forecast_adapter = LegacyForecastAdapter()
        self.ticket_builder = TicketBuilder()
        from core.decision.portfolio_intent import PortfolioIntentBuilder
        from core.release.strategy_bundle import StrategyReleaseLoader

        self.portfolio_intent_builder = PortfolioIntentBuilder()
        self.strategy_release_error = None
        self.strategy_release_bundle = strategy_release_bundle
        bundle_path = strategy_release_bundle_path or os.environ.get(
            "AI_BOT_STRATEGY_RELEASE_BUNDLE"
        )
        if self.strategy_release_bundle is None and bundle_path:
            try:
                self.strategy_release_bundle = StrategyReleaseLoader.load(Path(bundle_path))
            except Exception as exc:
                self.strategy_release_error = f"{type(exc).__name__}: {exc}"
                logging.exception("策略发布包校验失败，执行通道保持 shadow: %s", exc)
        report_path_value = profitability_report_path or os.environ.get(
            "AI_BOT_PROFITABILITY_REPORT"
        )
        manifest_path_value = candidate_release_manifest_path or os.environ.get(
            "AI_BOT_CANDIDATE_RELEASE_MANIFEST"
        )
        self.profitability_report_path = Path(report_path_value) if report_path_value else None
        self.candidate_release_manifest_path = (
            Path(manifest_path_value) if manifest_path_value else None
        )
        self.profitability_authorized, self.profitability_authorization_reason = (
            verify_candidate_authorization(
                self.profitability_report_path,
                self.candidate_release_manifest_path,
            )
        )
        self.profitability_manifest = None
        if self.profitability_authorized and self.candidate_release_manifest_path:
            self.profitability_manifest = json.loads(
                self.candidate_release_manifest_path.read_text(encoding="utf-8")
            )

    def _brain_authorized_for_ticket(
        self, prediction: Dict[str, Any], mode: str
    ) -> bool:
        # Name retained for compatibility; authorization now belongs to the
        # two-stage profitability model. Brain is always a rejected baseline.
        if self.required_brain_release_stage == "live":
            return False
        alpha = prediction.get("alpha_prediction")
        if not isinstance(alpha, dict):
            return False
        if str(alpha.get("model_family")) != "profitability_two_stage":
            return False
        if not bool(alpha.get("actionable")) or str(alpha.get("decision")) != "TRADE":
            return False
        if str(alpha.get("release_stage")) != "candidate":
            return False
        if str(alpha.get("profitability_gate")) != "PASSED":
            return False
        try:
            expected_horizon = horizon_for_mode(mode)
            if int(alpha.get("horizon_sec")) != expected_horizon:
                return False
        except (OverflowError, TypeError, ValueError):
            return False
        lower_edge = _finite_float(alpha.get("lower_bound_net_edge_bps"))
        if lower_edge is None or lower_edge <= 0:
            return False
        feature_evidence = alpha.get("feature_evidence")
        price_path = (
            feature_evidence.get("price_path")
            if isinstance(feature_evidence, dict)
            else None
        )
        try:
            observed_bar_count = int(price_path.get("observed_bar_count"))
            interval_sec = int(price_path.get("interval_sec"))
        except (AttributeError, OverflowError, TypeError, ValueError):
            return False
        age_seconds = _finite_float(price_path.get("age_seconds"))
        maximum_age_seconds = _finite_float(
            price_path.get("maximum_age_seconds")
        )
        expected_maximum_age = float(
            MAX_CANDIDATE_KLINE_AGE_SEC[expected_horizon]
        )
        if (
            not isinstance(price_path, dict)
            or price_path.get("status") != "verified"
            or price_path.get("training_kline_source") != "bybit"
            or price_path.get("runtime_price_source")
            != "bybit_linear_last_trade_kline"
            or price_path.get("same_venue") is not True
            or price_path.get("continuous") is not True
            or price_path.get("ohlcv_contract_valid") is not True
            or observed_bar_count < 49
            or interval_sec != expected_horizon
            or price_path.get("candidate_freshness_verified") is not True
            or age_seconds is None
            or maximum_age_seconds is None
            or age_seconds < 0
            or maximum_age_seconds <= 0
            or age_seconds > maximum_age_seconds
            or not math.isclose(
                maximum_age_seconds,
                expected_maximum_age,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
        ):
            return False
        if not self.profitability_authorized or not self.profitability_manifest:
            return False
        if str(alpha.get("release_id")) != str(self.profitability_manifest.get("release_id")):
            return False
        if str(alpha.get("model_artifact_sha256") or "") != str(
            self.profitability_manifest.get("model_artifact_sha256") or ""
        ):
            return False
        if str(alpha.get("lockbox_fingerprint") or "") != str(
            self.profitability_manifest.get("lockbox_fingerprint") or ""
        ):
            return False
        bundle = self.strategy_release_bundle
        if bundle is None or bundle.release_stage != "candidate":
            return False
        declared_release = str(
            alpha.get("strategy_release_id") or prediction.get("strategy_release_id") or ""
        )
        return declared_release == bundle.strategy_release_id

    def _get_file_path(self, symbol: str, mode: str) -> Path:
        return self.results_dir / f"{symbol}_{mode}.json"

    def _get_training_meta_path(self, symbol: str, mode: str) -> Path:
        return self.results_dir / f"{symbol}_{mode}_training.json"

    async def save_result(self, symbol: str, mode: str, data: dict):
        """保存预测结果。除原始字段外，自动补齐扩展字段。"""
        normalized = _normalize_prediction(data, saving=True)
        # 顺手把训练元数据合并进来，便于 API 一次返回
        meta = self.load_training_metadata(symbol, mode)
        if meta:
            normalized["training_metadata"] = meta
        file_path = self._get_file_path(symbol, mode)
        await asyncio.to_thread(_atomic_json_write, file_path, normalized)
        try:
            forecast = self.forecast_adapter.adapt(symbol, mode, normalized)
            await asyncio.to_thread(self.control_plane.publish, forecast, None)
            ticket = None
            portfolio_intent = None
            if self.tickets_enabled and self._brain_authorized_for_ticket(
                normalized, mode
            ):
                reference_price = (
                    normalized.get("current_price")
                    or normalized.get("kline_last_price")
                    or normalized.get("last")
                )
                reference_price = _finite_float(reference_price)
                if reference_price is not None and reference_price > 0:
                    release_id = self.strategy_release_bundle.strategy_release_id
                    forecasts = await asyncio.to_thread(
                        self.control_plane.active_forecasts,
                        symbol,
                        strategy_release_id=release_id,
                    )
                    decision_version = await asyncio.to_thread(
                        self.control_plane.next_portfolio_decision_version, symbol
                    )
                    portfolio_intent = self.portfolio_intent_builder.build(
                        forecasts,
                        strategy_release_id=release_id,
                        decision_version=decision_version,
                    )
                    latest_intent = await asyncio.to_thread(
                        self.control_plane.latest_portfolio_intent, symbol
                    )
                    if portfolio_intent and latest_intent:
                        previous_sources = {
                            (item.forecast_id, item.forecast_revision)
                            for item in latest_intent.contributions
                        }
                        new_sources = {
                            (item.forecast_id, item.forecast_revision)
                            for item in portfolio_intent.contributions
                        }
                        if previous_sources == new_sources:
                            portfolio_intent = None
                    position_version = await asyncio.to_thread(
                        self.control_plane.latest_position_version, symbol
                    )
                    if portfolio_intent:
                        ticket = self.ticket_builder.build_portfolio_ticket(
                            portfolio_intent,
                            forecasts,
                            reference_price=reference_price,
                            required_position_version=position_version,
                        )
            if ticket and portfolio_intent:
                await asyncio.to_thread(
                    self.control_plane.publish, forecast, ticket, portfolio_intent
                )
        except Exception as exc:
            # The legacy prediction remains available, but ticket generation is fail-closed.
            logging.exception("预测契约/操作票落盘失败，已禁止该结果进入执行通道: %s", exc)
        logging.info(f"已保存 {symbol} 在 {mode} 时间粒度下的预测结果到 {file_path}")

    def save_training_metadata(self, symbol: str, mode: str, metadata: Dict[str, Any]) -> None:
        """落盘每个模式的训练元数据：开始/结束/耗时/窗口/horizon/样本/校验等。"""
        try:
            metadata = dict(metadata or {})
            metadata.setdefault("symbol", symbol)
            metadata.setdefault("mode", mode)
            metadata.setdefault("generated_at", _now_iso())
            path = self._get_training_meta_path(symbol, mode)
            _atomic_json_write(path, metadata)
        except Exception as exc:
            logging.warning(f"训练元数据落盘失败: {exc}")

    def load_training_metadata(self, symbol: str, mode: str) -> Optional[Dict[str, Any]]:
        path = self._get_training_meta_path(symbol, mode)
        try:
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def get_latest_results(self) -> dict:
        """读取并聚合所有已保存的最新结果，返回完整字典结构。"""
        all_results: Dict[str, Any] = {}
        for file_path in self.results_dir.glob("*.json"):
            # 跳过训练元数据
            if file_path.stem.endswith("_training"):
                continue
            try:
                parts = file_path.stem.split('_', 1)
                if len(parts) != 2:
                    logging.warning(f"跳过格式不正确的文件名: {file_path.name}")
                    continue
                symbol, mode = parts
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                data = _normalize_prediction(data, saving=False)
                if symbol not in all_results:
                    all_results[symbol] = {"details": {}, "recommendation": None}
                all_results[symbol]["details"][mode] = data
            except json.JSONDecodeError:
                logging.error(f"解析 JSON 文件出错: {file_path}")
            except Exception as e:
                logging.error(f"读取文件时发生错误 {file_path}: {e}")

        # 顶层推荐：按 score 选最强模式
        for symbol, data in all_results.items():
            if data["details"]:
                best_mode = None
                max_score = -1
                for mode_name, mode_data in data["details"].items():
                    s = mode_data.get("score")
                    try:
                        if s is not None and float(s) > max_score:
                            max_score = float(s)
                            best_mode = mode_name
                    except Exception:
                        continue
                data["recommendation"] = best_mode
        return all_results
