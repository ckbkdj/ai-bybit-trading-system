# 这个类负责保存每个模型的独立预测结果，并能聚合所有最新结果
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import asyncio

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from core.control_plane import ControlPlaneRepository
from core.decision.ticket_builder import TicketBuilder


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


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
    "trade_direction",
    "trade_actionable",
    "target_leverage",
    "target_raw_return",
    "expected_leveraged_return",
    "brain_training",
)


def _normalize_prediction(data: Dict[str, Any]) -> Dict[str, Any]:
    """补齐扩展字段；保留旧字段含义。"""
    if not isinstance(data, dict):
        return {"error": "invalid prediction payload"}
    out = dict(data)
    out.setdefault("generated_at", _now_iso())
    # saved_at / updated_at 始终刷新为当前时间，便于落盘 & API 输出感知最新一次归一化
    _ts_now = _now_iso()
    out["saved_at"] = _ts_now
    out["updated_at"] = _ts_now
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


class ResultManager:
    def __init__(
        self,
        results_dir: Path,
        *,
        control_plane_db: Path | None = None,
        tickets_enabled: bool | None = None,
    ):
        self.results_dir = results_dir
        self.results_dir.mkdir(parents=True, exist_ok=True)
        if tickets_enabled is None:
            tickets_enabled = os.environ.get("AI_BOT_TICKETS_ENABLED", "true").strip().lower() in {
                "1", "true", "yes", "on"
            }
        self.tickets_enabled = bool(tickets_enabled)
        default_db = self.results_dir.parent / "data" / "control_plane.sqlite3"
        self.control_plane = ControlPlaneRepository(control_plane_db or default_db)
        self.forecast_adapter = LegacyForecastAdapter()
        self.ticket_builder = TicketBuilder()

    def _get_file_path(self, symbol: str, mode: str) -> Path:
        return self.results_dir / f"{symbol}_{mode}.json"

    def _get_training_meta_path(self, symbol: str, mode: str) -> Path:
        return self.results_dir / f"{symbol}_{mode}_training.json"

    async def save_result(self, symbol: str, mode: str, data: dict):
        """保存预测结果。除原始字段外，自动补齐扩展字段。"""
        normalized = _normalize_prediction(data)
        # 顺手把训练元数据合并进来，便于 API 一次返回
        meta = self.load_training_metadata(symbol, mode)
        if meta:
            normalized["training_metadata"] = meta
        file_path = self._get_file_path(symbol, mode)
        await asyncio.to_thread(
            file_path.write_text,
            json.dumps(normalized, indent=2, ensure_ascii=False),
        )
        try:
            forecast = self.forecast_adapter.adapt(symbol, mode, normalized)
            ticket = None
            if self.tickets_enabled:
                reference_price = (
                    normalized.get("current_price")
                    or normalized.get("kline_last_price")
                    or normalized.get("last")
                )
                try:
                    reference_price = float(reference_price)
                except (TypeError, ValueError):
                    reference_price = 0.0
                if reference_price > 0:
                    position_version = await asyncio.to_thread(
                        self.control_plane.latest_position_version, symbol
                    )
                    ticket = self.ticket_builder.build_open_ticket(
                        forecast,
                        reference_price=reference_price,
                        required_position_version=position_version,
                    )
            await asyncio.to_thread(self.control_plane.publish, forecast, ticket)
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
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
            tmp.replace(path)
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
                data = _normalize_prediction(data)
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
