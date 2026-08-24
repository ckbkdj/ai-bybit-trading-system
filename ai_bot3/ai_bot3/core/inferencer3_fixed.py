import asyncio
import logging
import math
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# 必须在 `import keras` / `from keras.models import load_model` 之前把后端、
# 线程上限、GPU 内存增长等环境变量设置好，否则 keras / TF 会按默认值固化
# 线程池，再调用 tf.config.threading.set_*_parallelism_threads() 也无效。
# ---------------------------------------------------------------------------
if os.environ.get("AI_BOT_FORCE_CPU") == "1":
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
os.environ.setdefault("KERAS_BACKEND", "tensorflow")  # keras 3.x 后推荐
os.environ.setdefault("OMP_NUM_THREADS", "2")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("TF_NUM_INTRAOP_THREADS", "2")
os.environ.setdefault("TF_NUM_INTEROP_THREADS", "1")
os.environ.setdefault("TF_FORCE_GPU_ALLOW_GROWTH", "true")

import keras
import numpy as np
import pandas as pd
import talib
from sklearn.metrics import mean_squared_error

from keras.models import load_model

from .market_context import (
    MARKET_FEATURE_COLUMNS,
    NEWS_FEATURE_COLUMNS,
    assess_context_completeness,
    compute_market_bias,
    fuse_direction_signals,
)
from .data_fetch import MarketDataUnavailable
from .brain_model import predict_brain_from_df
from .kline_feature_store import (
    KLINE_DERIVED_FEATURES,
    add_kline_derived_features,
    select_persisted_features,
)
from .model_monitoring import factor_group_scores, scaled_feature_ood_score, source_is_reliable
from .models.profitability_runtime import (
    generate_profitability_alpha_prediction,
    price_frame_cutoff,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _safe_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except Exception:
        return default


def _configure_tf_runtime(log: logging.Logger) -> None:
    """Configure TF threading + GPU memory growth in the worker process.

    Must be called *before* the first `load_model` / `predict`, otherwise the
    GPU/threading defaults will be locked in. Errors are downgraded to warnings
    so that a stale TF state never breaks inference.
    """
    try:
        import tensorflow as tf
    except Exception as exc:  # pragma: no cover - defensive
        log.warning(f"tensorflow import 失败，跳过 GPU/线程配置: {exc}")
        return
    try:
        gpus = tf.config.experimental.list_physical_devices('GPU')
        if gpus:
            for gpu in gpus:
                try:
                    tf.config.experimental.set_memory_growth(gpu, True)
                except RuntimeError as _e:
                    log.warning(f"set_memory_growth 已无法修改 (TF 已初始化): {_e}")
            log.info(f"Keras inference: GPU devices visible: {gpus}")
        else:
            log.info("Keras inference: 未检测到可用 GPU，使用 CPU 回退")
    except Exception as exc:
        log.warning(f"列出 GPU 设备失败: {exc}")
    _intra = _safe_int_env("TF_NUM_INTRAOP_THREADS", 2)
    _inter = _safe_int_env("TF_NUM_INTEROP_THREADS", 1)
    try:
        tf.config.threading.set_intra_op_parallelism_threads(_intra)
    except RuntimeError as _e:
        log.warning(f"set_intra_op_parallelism_threads 已无法修改: {_e}")
    try:
        tf.config.threading.set_inter_op_parallelism_threads(_inter)
    except RuntimeError as _e:
        log.warning(f"set_inter_op_parallelism_threads 已无法修改: {_e}")

FEATURE_FETCH_TIMEOUT = float(os.environ.get("FEATURE_FETCH_TIMEOUT", "45"))
OHLCV_FETCH_TIMEOUT = float(os.environ.get("OHLCV_FETCH_TIMEOUT", "60"))


def _fresh_liq_current_price(sym: str) -> Dict[str, Any]:
    """Read current price from local Coinglass liquidation map lastPrice with freshness metadata."""
    max_age = float(os.environ.get("LIQ_CURRENT_PRICE_MAX_AGE_SECONDS", "600"))
    base = sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
    path = Path(__file__).resolve().parent.parent / "data" / f"{base}.json"
    meta: Dict[str, Any] = {
        "current_price": None,
        "current_price_source": "coinglass_liqmap_missing",
        "current_price_mtime": None,
        "current_price_age_seconds": None,
        "current_price_warning": None,
    }
    try:
        if not path.exists():
            meta["current_price_warning"] = f"缺少爆仓图当前价格文件: {path.name}"
            return meta
        mtime = path.stat().st_mtime
        age = max(0.0, datetime.now().timestamp() - mtime)
        meta["current_price_mtime"] = datetime.fromtimestamp(mtime, timezone.utc).astimezone().isoformat()
        meta["current_price_age_seconds"] = age
        import json as _json
        with path.open("r", encoding="utf-8") as f:
            payload = _json.load(f)
        if isinstance(payload, str):
            payload = _json.loads(payload)
        price = float((payload or {}).get("lastPrice") or 0.0)
        if price <= 0:
            meta["current_price_source"] = "coinglass_liqmap_invalid"
            meta["current_price_warning"] = "爆仓图 lastPrice 无效"
            return meta
        meta["current_price"] = price
        if age > max_age:
            meta["current_price_source"] = "coinglass_liqmap_stale"
            meta["current_price_warning"] = f"爆仓图当前价格过期: age_sec={int(age)}, max_age_sec={int(max_age)}"
        else:
            meta["current_price_source"] = "coinglass_liqmap"
        return meta
    except Exception as exc:
        meta["current_price_source"] = "coinglass_liqmap_error"
        meta["current_price_warning"] = f"爆仓图当前价格读取失败: {type(exc).__name__}: {exc}"
        return meta

def _safe_scalar(value, default: float, log: logging.Logger, name: str) -> float:
    if isinstance(value, Exception):
        log.warning(f"{name} 获取失败，使用默认值 {default}: {value}")
        return float(default)
    if value is None:
        log.warning(f"{name} 返回 None，使用默认值 {default}")
        return float(default)
    try:
        return float(value)
    except Exception:
        log.warning(f"{name} 非法值 {value!r}，使用默认值 {default}")
        return float(default)

def safe_load_model(model_path):
    model_path = Path(model_path)
    for path in [model_path, model_path.with_suffix('.keras.bak'), model_path.with_suffix('.keras.bak2')]:
        if path.exists():
            try:
                return load_model(path, compile=False)
            except Exception as e:
                print(f"模型损坏: {path}, 尝试下一个。原因: {e}")
                continue
    raise RuntimeError("模型损坏且无任何可用备份！")

def _calculate_metrics(
    y_pred_inverse: np.ndarray,
    y_actual_inverse: np.ndarray,
    last_price: float,
    sym: str,
    tf_code: str,
    *,
    extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    last_price = float(last_price)
    rmse = float(math.sqrt(mean_squared_error(y_actual_inverse, y_pred_inverse)))
    pred_value = float(y_pred_inverse[-1, 0])
    ci_hw = 1.96 * rmse
    trend = "flat"
    if pred_value > last_price * 1.0005:
        trend = "up"
    elif pred_value < last_price * 0.9995:
        trend = "down"
    score = abs(pred_value - last_price) / ci_hw if ci_hw > 1e-9 else 0.0
    predicted_return = (pred_value - last_price) / last_price if last_price else 0.0
    trade_direction = "long" if trend == "up" else "short" if trend == "down" else "flat"
    trade_predicted_return = -predicted_return if trade_direction == "short" else predicted_return if trade_direction == "long" else 0.0
    out: Dict[str, Any] = {
        "generated_at": _now_iso(),
        "timeframe": tf_code,
        "pred": pred_value,
        "last": last_price,
        "ci": [pred_value - ci_hw, pred_value + ci_hw],
        "trend": trend,
        "raw_trend": trend,
        "price_trend": trend,
        "score": score,
        "rmse": rmse,
        "symbol": sym,
        # price return is signed by price direction; trade return is PnL by chosen side.
        "predicted_return": predicted_return,
        "raw_predicted_return": predicted_return,
        "price_predicted_return": predicted_return,
        "display_price_return": predicted_return,
        "trade_direction": trade_direction,
        "trade_predicted_return": trade_predicted_return,
        "display_trade_return": trade_predicted_return,
        "model_version": "lstm_keras_v2_purged_holdout",
    }
    if extras:
        out.update(extras)
    # 把市场因子 + 在线学习 + LLM 辅助合并
    snapshot = (extras or {}).get("market_snapshot") or {}
    completeness = (extras or {}).get("context_completeness") or {"score": 0.0}
    bias = compute_market_bias(snapshot, completeness)
    out["factor_bias"] = bias["factor_bias"]
    out["weights"] = bias["weights"]
    out["news_weight_total"] = bias["news_weight_total"]
    out["context_completeness"] = bias["context_completeness"]

    llm_payload = (extras or {}).get("openai_prediction") or {}
    llm_signal = float(llm_payload.get("score") or 0.0)
    news_signal = float(bias["components"].get("news_signal") or 0.0)

    fused = fuse_direction_signals(
        local_predicted_return=predicted_return,
        factor_bias=bias["factor_bias"],
        news_signal=news_signal,
        llm_signal=llm_signal,
        completeness=completeness,
        llm_available=str(llm_payload.get("status") or "").lower() == "ok",
    )
    out["ensemble_score"] = fused["fused_score"]
    out["fused_weights"] = fused["fused_weights"]
    out["calibrated_trend"] = fused["direction"]
    out["confidence"] = min(1.0, abs(fused["fused_score"]))
    out["llm_signal"] = llm_signal
    out["openai_prediction"] = llm_payload or None
    out["factor_scores"] = factor_group_scores(snapshot, bias["factor_bias"], llm_signal)

    # 在线学习校准（如有）：不覆盖 raw_predicted_return / predicted_return / calibrated_trend，
    # 仅写入独立的 calibrated_* 字段，保持价格展示字段与 pred/last 一致。
    cal = (extras or {}).get("online_calibration")
    if isinstance(cal, dict):
        out["calibrated_predicted_return"] = cal.get("calibrated_predicted_return", predicted_return)
        out["calibrated_return"] = cal.get("calibrated_predicted_return", predicted_return)
        out["calibrated_direction"] = cal.get("calibrated_trend", out.get("calibrated_trend"))
        out["direction_confidence"] = cal.get("direction_confidence", out["confidence"])
        out["calibration_status"] = str(cal.get("calibration_status") or "unknown")
        out["online_learning"] = cal.get("online_learning")
    else:
        out["calibration_status"] = "unknown"
    return out

def run_onnx_inference_in_process(prepared_data: Dict[str, Any]) -> Dict[str, Any] | None:
    try:
        import onnxruntime as ort
    except ImportError:
        logging.getLogger(__name__).error("ONNX Runtime 未安装，无法执行推理。")
        return None

    onnx_path_str = Path(prepared_data['model_path_str']).with_suffix(".onnx").as_posix()
    scaler_path_str = prepared_data['scaler_path_str']
    X_seq = prepared_data['X_seq']
    y_seq_scaled = prepared_data['y_seq_scaled']
    last_price = prepared_data['last_price']
    sym = prepared_data['sym']
    tf_code = prepared_data['tf_code']
    mode = prepared_data.get('mode') or tf_code

    log = logging.getLogger(f"Worker.ONNX.{sym}.{tf_code}")
    log.info("ONNX 子进程已启动。")

    sess = ort.InferenceSession(onnx_path_str, providers=['CPUExecutionProvider'])
    input_name = sess.get_inputs()[0].name
    y_pred_scaled = sess.run(None, {input_name: X_seq.astype(np.float32)})[0]

    with open(scaler_path_str, "rb") as f:
        _, scaler_y = pickle.load(f)

    y_pred_inverse = scaler_y.inverse_transform(y_pred_scaled)
    y_actual_inverse = scaler_y.inverse_transform(y_seq_scaled)

    extras = {
        "market_data_source": prepared_data.get("market_data_source"),
        "data_source_status": prepared_data.get("data_source_status"),
        "latest_kline_ts": prepared_data.get("latest_kline_ts"),
        "market_data_fetched_at": prepared_data.get("market_data_fetched_at"),
        "market_data_new_candles": prepared_data.get("market_data_new_candles"),
        "kline_last_price": prepared_data.get("kline_last_price"),
        "current_price": prepared_data.get("current_price"),
        "current_price_source": prepared_data.get("current_price_source"),
        "current_price_mtime": prepared_data.get("current_price_mtime"),
        "current_price_age_seconds": prepared_data.get("current_price_age_seconds"),
        "current_price_warning": prepared_data.get("current_price_warning"),
    }
    loaded_model_metadata = prepared_data.get("loaded_model_metadata") or {}
    validation_metadata = loaded_model_metadata.get("validation") or {}
    verified_weak_prior = bool(
        loaded_model_metadata.get("model_version") == "lstm_keras_v2_purged_holdout"
        and validation_metadata.get("holdout_seen_during_fit") is False
    )
    extras["model_version"] = (
        "lstm_keras_v2_purged_holdout" if verified_weak_prior else "legacy_unverified_lstm"
    )
    extras["loaded_model_metadata"] = loaded_model_metadata or None
    result = _calculate_metrics(y_pred_inverse, y_actual_inverse, last_price, sym, tf_code, extras=extras)
    for k in (
        "market_data_source",
        "data_source_status",
        "latest_kline_ts",
        "market_data_fetched_at",
        "market_data_new_candles",
        "kline_last_price",
        "current_price",
        "current_price_source",
        "current_price_mtime",
        "current_price_age_seconds",
        "current_price_warning",
    ):
        result[k] = extras.get(k)
    return result

def run_keras_inference_in_process(prepared_data: Dict[str, Any]) -> Dict[str, Any] | None:
    model_path_str = prepared_data['model_path_str']
    scaler_path_str = prepared_data['scaler_path_str']
    X_seq = prepared_data['X_seq']
    y_seq_scaled = prepared_data['y_seq_scaled']
    last_price = prepared_data['last_price']
    sym = prepared_data['sym']
    tf_code = prepared_data['tf_code']
    mode = prepared_data.get('mode') or tf_code

    log = logging.getLogger(f"Worker.Keras.{sym}.{tf_code}")
    log.info("Keras 回退子进程已启动。")

    # 在第一次 load_model / predict 之前配置 TF 线程数与 GPU 内存增长，
    # 避免 forecast 子进程拉起后用默认线程池占满 CPU。
    _configure_tf_runtime(log)

    model = safe_load_model(model_path_str)
    y_pred_scaled = model.predict(X_seq, verbose=0)

    with open(scaler_path_str, "rb") as f:
        bundle = pickle.load(f)
    if isinstance(bundle, tuple) and len(bundle) >= 2:
        scaler_X = bundle[0]
        scaler_y = bundle[1]
    else:
        scaler_X = None
        scaler_y = bundle

    ood = scaled_feature_ood_score(X_seq[-1], scaler_X) if scaler_X is not None else None
    source_reliable = source_is_reliable(
        prepared_data.get("data_source_status"),
        prepared_data.get("current_price_age_seconds"),
    )

    y_pred_inverse = scaler_y.inverse_transform(y_pred_scaled)
    y_actual_inverse = scaler_y.inverse_transform(y_seq_scaled)

    extras = {
        "market_snapshot": prepared_data.get("market_snapshot") or {},
        "context_completeness": prepared_data.get("context_completeness") or {"score": 0.0},
        "openai_prediction": prepared_data.get("openai_prediction"),
        "online_calibration": prepared_data.get("online_calibration"),
        "data_sources_generated_at": prepared_data.get("data_sources_generated_at") or {},
        "external_panel_context": prepared_data.get("external_panel_context"),
        # 行情数据源溯源（来自 df.attrs，由 data_fetch.get_ohlcv 写入）
        "market_data_source": prepared_data.get("market_data_source"),
        "data_source_status": prepared_data.get("data_source_status"),
        "latest_kline_ts": prepared_data.get("latest_kline_ts"),
        "market_data_fetched_at": prepared_data.get("market_data_fetched_at"),
        "market_data_new_candles": prepared_data.get("market_data_new_candles"),
        "kline_last_price": prepared_data.get("kline_last_price"),
        "current_price": prepared_data.get("current_price"),
        "current_price_source": prepared_data.get("current_price_source"),
        "current_price_mtime": prepared_data.get("current_price_mtime"),
        "current_price_age_seconds": prepared_data.get("current_price_age_seconds"),
        "current_price_warning": prepared_data.get("current_price_warning"),
        "data_source_reliable": source_reliable,
        "range_guard_score": ood.score if ood is not None else 1.0,
        "range_guard_details": (
            {
                "method": ood.method,
                "violation_fraction": ood.violation_fraction,
                "maximum_excess": ood.maximum_excess,
            }
            if ood is not None
            else {"method": "missing_feature_scaler"}
        ),
    }
    loaded_model_metadata = prepared_data.get("loaded_model_metadata") or {}
    validation_metadata = loaded_model_metadata.get("validation") or {}
    verified_weak_prior = bool(
        loaded_model_metadata.get("model_version") == "lstm_keras_v2_purged_holdout"
        and validation_metadata.get("holdout_seen_during_fit") is False
    )
    extras["model_version"] = (
        "lstm_keras_v2_purged_holdout" if verified_weak_prior else "legacy_unverified_lstm"
    )
    extras["loaded_model_metadata"] = loaded_model_metadata or None
    result = _calculate_metrics(y_pred_inverse, y_actual_inverse, last_price, sym, tf_code, extras=extras)
    try:
        brain_df = prepared_data.get("brain_df")
        brain_pred = predict_brain_from_df(
            brain_df, sym, mode,
            cfg=prepared_data.get("cfg") or {},
            market_snapshot=extras.get("market_snapshot") or {},
            local_predicted_return=(result.get("predicted_return") if verified_weak_prior else None),
        ) if brain_df is not None else {"status": "missing_features", "direction": "flat", "actionable": False}
        result["brain_prediction"] = brain_pred
        result["trade_actionable"] = bool(brain_pred.get("actionable"))
        result["target_leverage"] = brain_pred.get("leverage")
        result["target_raw_return"] = brain_pred.get("target_raw_return")
        result["target_leveraged_profit"] = brain_pred.get("target_leveraged_profit")
        result["expected_leveraged_return"] = brain_pred.get("expected_leveraged_return")
        # 达标/收益展示口径：只根据预测价和当前价的涨跌计算收益。
        # 涨：收益=(pred-last)/last；跌：收益=(last-pred)/last。Brain 不覆盖收益。
        try:
            _lev = float(brain_pred.get("leverage") or 1.0)
            _target_lev_profit = float(brain_pred.get("target_leveraged_profit") or 0.31)
            _price_ret = float(result.get("display_price_return") or result.get("price_predicted_return") or result.get("predicted_return") or 0.0)
            if _price_ret < -0.0005:
                _trade_ret = -_price_ret
                _trade_dir = "short"
            elif _price_ret > 0.0005:
                _trade_ret = _price_ret
                _trade_dir = "long"
            else:
                _trade_ret = 0.0
                _trade_dir = "flat"
            _trade_lev_ret = _trade_ret * _lev
            result["trade_direction"] = _trade_dir
            result["trade_return_direction"] = _trade_dir
            result["display_trade_return"] = _trade_ret
            result["trade_predicted_return"] = _trade_ret
            result["display_trade_leveraged_return"] = _trade_lev_ret
            result["trade_leveraged_return"] = _trade_lev_ret
            result["trade_target_met"] = bool(_trade_lev_ret >= _target_lev_profit)
        except Exception:
            pass
        # 如果 Brain 有高置信可交易方向，只作为独立 brain_trend 参考；不覆盖收益和交易方向。
        if brain_pred.get("actionable"):
            _btrend = "up" if brain_pred.get("direction") == "long" else "down" if brain_pred.get("direction") == "short" else "flat"
            result["brain_trend"] = _btrend
            result["confidence"] = max(float(result.get("confidence") or 0.0), float(brain_pred.get("confidence") or 0.0))
    except Exception as exc:
        result["brain_prediction"] = {"status": "error", "direction": "flat", "actionable": False, "error": str(exc)}
    result["alpha_prediction"] = generate_profitability_alpha_prediction(
        (
            prepared_data.get("alpha_price_frame")
            if prepared_data.get("alpha_price_frame") is not None
            else pd.DataFrame()
        ),
        symbol=sym,
        mode=mode,
        input_price_source=prepared_data.get("alpha_price_source"),
        external_panel_context=prepared_data.get("external_panel_context"),
    )
    if (
        result["alpha_prediction"].get("status") != "ok"
        and prepared_data.get("alpha_price_error")
    ):
        result["alpha_prediction"]["price_source_error"] = prepared_data.get(
            "alpha_price_error"
        )
    # Brain remains visible as a rejected comparison baseline.  Only the new
    # profitability Alpha may mark a production result actionable.
    result["trade_actionable"] = bool(result["alpha_prediction"].get("actionable"))
    result["external_panel_context"] = extras["external_panel_context"]
    result["data_sources_generated_at"] = extras["data_sources_generated_at"]
    # 把行情溯源字段固化进 JSON 结果，便于 API/前端展示与陈旧检测。
    for k in (
        "market_data_source",
        "data_source_status",
        "latest_kline_ts",
        "market_data_fetched_at",
        "market_data_new_candles",
        "kline_last_price",
        "current_price",
        "current_price_source",
        "current_price_mtime",
        "current_price_age_seconds",
        "current_price_warning",
    ):
        result[k] = extras.get(k)
    return result

class InferencerDataPreparer:
    def __init__(
        self,
        sym: str,
        tf_code: str,
        limits: Tuple[int, int],
        model_dir: Path,
        fetcher: Any,
        sentiment: Any,
        cfg: Dict[str, Any] | None = None,
        mode: str | None = None,
        llm_aux: Any = None,
        calibrator: Any = None,
    ):
        self.sym, self.tf_code = sym, tf_code
        self.limit, self.window = limits
        # 预测只需要最近 window + 指标 warmup 的一小段 K 线；训练可以用 3 年全量。
        # 之前这里直接按 modes.limit 读取，3m=527040 行，导致预测进程内存暴涨并被 OOM killer 杀掉。
        self.fetch_limit = int(max(int(self.window) + 240, int(self.window) * 3, 512))
        self.fetcher, self.sentiment = fetcher, sentiment
        self.cfg = cfg or {}
        self.mode = mode or tf_code
        self.llm_aux = llm_aux
        self.calibrator = calibrator
        tag = f"{sym}_{tf_code}"
        self.model_path = model_dir / f"{tag}.keras"
        self.scaler_path = model_dir / f"{tag}_scaler.pkl"
        self.log = logging.getLogger(f"DataPrep.Inferencer.{sym}.{tf_code}")


    async def _feature_df(self) -> pd.DataFrame:
        try:
            df = await asyncio.wait_for(
                self.fetcher.get_ohlcv(self.sym, self.tf_code, self.fetch_limit),
                timeout=OHLCV_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self.log.error(f"get_ohlcv 超时（{OHLCV_FETCH_TIMEOUT}s）: {self.sym}-{self.tf_code}")
            return pd.DataFrame()
        except MarketDataUnavailable as e:
            # 生产规则：数据源 451/陈旧时，不允许继续。向上抛由 prepare_data 处理。
            self.log.error(
                f"市场数据不可用，拒绝产出预测: {self.sym}-{self.tf_code} "
                f"source={e.source} status={e.status} reason={e.reason} latest_ts={e.latest_ts}"
            )
            raise
        except Exception as e:
            self.log.error(f"get_ohlcv 失败: {e}", exc_info=True)
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        df["ma"] = talib.SMA(df["close"], 14)
        df["rsi"] = talib.RSI(df["close"], 14)
        up, mid, low = talib.BBANDS(df["close"], 20)
        df[["boll_upper", "boll_middle", "boll_lower"]] = np.column_stack((up, mid, low))
        macd, sig, hist = talib.MACD(df["close"], 12, 26, 9)
        df[["macd", "macdsignal", "macdhist"]] = np.column_stack((macd, sig, hist))

        try:
            fr, ls, news = await asyncio.wait_for(
                asyncio.gather(
                    self.fetcher.funding_rate(self.sym),
                    self.fetcher.long_short_ratio(self.sym, "2h"),
                    self.sentiment.score(self.sym),
                    return_exceptions=True,
                ),
                timeout=FEATURE_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self.log.error(
                f"附加特征获取超时（{FEATURE_FETCH_TIMEOUT}s）: {self.sym}-{self.tf_code}，"
                f"不再降级使用中性值，拒绝产出预测。"
            )
            raise MarketDataUnavailable(
                self.sym, self.tf_code,
                source="binance_futures",
                status="fetch_failed",
                reason=f"feature gather timeout after {FEATURE_FETCH_TIMEOUT}s",
            )
        except Exception as e:
            self.log.error(f"附加特征获取失败: {e}", exc_info=True)
            raise MarketDataUnavailable(
                self.sym, self.tf_code,
                source="binance_futures",
                status="fetch_failed",
                reason=f"feature gather error: {type(e).__name__}: {e}",
            ) from e

        # 关键因子（funding/ls）不允许伪造中性值。若是 MarketDataUnavailable
        # 就直接放弃本轮预测；其他异常仍允许 _safe_scalar 走默认。
        for val, name in ((fr, "funding_rate"), (ls, "long_short_ratio")):
            if isinstance(val, MarketDataUnavailable):
                self.log.error(
                    f"{name} 不可用，拒绝产出预测: {self.sym} "
                    f"status={val.status} reason={val.reason}"
                )
                raise val

        df["funding_rate"] = _safe_scalar(fr, 0.0, self.log, "funding_rate")
        df["long_short_ratio"] = _safe_scalar(ls, 1.0, self.log, "long_short_ratio")
        df["news_sentiment"] = _safe_scalar(news, 0.0, self.log, "news_sentiment")

        # 保留扩展多因子列以维持 scaler 列对齐（如果 scaler 是新版 30+ 列）
        try:
            snap = self.fetcher.build_local_snapshot(self.sym)
        except Exception:
            snap = {}
        for col in MARKET_FEATURE_COLUMNS + NEWS_FEATURE_COLUMNS:
            if col in df.columns:
                continue
            df[col] = float(snap.get(col, 0.0))

        # 与训练共用同一套 K 线派生特征，并保持训练期的一根 K 线反泄漏位移。
        # 旧模型若未使用这些列不会受到影响；新版 scaler 会按持久化列名取值。
        df = add_kline_derived_features(df)
        df[list(KLINE_DERIVED_FEATURES)] = df[list(KLINE_DERIVED_FEATURES)].shift(1)

        # 保留行情数据源元信息，供 prepare_data / inference 透传到结果。
        src_attrs = dict(getattr(df, "attrs", {}) or {})

        df.dropna(inplace=True)
        if df.empty:
            return pd.DataFrame()

        num_cols = df.select_dtypes(include=np.number).columns
        df[num_cols] = df[num_cols].astype(np.float32)
        # dropna 等操作会丢失 attrs，重新挂回。
        try:
            df.attrs.update(src_attrs)
        except Exception:
            pass
        return df

    async def prepare_data_for_process(self) -> Dict[str, Any] | None:
        if not self.scaler_path.exists():
            self.log.error(f"找不到缩放器 {self.scaler_path}，无法预测。")
            return None

        df = await self._feature_df()
        if df.empty or len(df) < self.window:
            return None

        alpha_price_frame = pd.DataFrame()
        alpha_price_error: str | None = None
        if os.environ.get("AI_BOT_PROFITABILITY_MODEL_BUNDLE"):
            try:
                alpha_price_frame = await asyncio.wait_for(
                    self.fetcher.get_bybit_ohlcv(
                        self.sym,
                        self.tf_code,
                        max(self.fetch_limit, 360),
                    ),
                    timeout=OHLCV_FETCH_TIMEOUT,
                )
            except Exception as exc:
                alpha_price_error = f"{type(exc).__name__}: {exc}"
                self.log.error(
                    "Bybit Alpha 行情不可用；旧模型仅展示，新 Alpha 失败关闭: %s",
                    alpha_price_error,
                )

        try:
            with open(self.scaler_path, "rb") as f:
                bundle = pickle.load(f)
            if isinstance(bundle, tuple) and len(bundle) == 3:
                scaler_X, scaler_y, persisted_features = bundle
            else:
                scaler_X, scaler_y = bundle
                persisted_features = None
        except Exception as e:
            self.log.error(f"加载 scaler 失败: {e}", exc_info=True)
            return None

        # 优先复用训练时保存的列顺序，避免特征演进时维度错位
        default_features = ["open", "high", "low", "close", "volume", "ma", "rsi", "boll_upper", "boll_middle", "boll_lower",
                            "macd", "macdsignal", "macdhist", "funding_rate", "long_short_ratio", "news_sentiment"]
        features = list(persisted_features) if persisted_features else default_features
        try:
            X_raw = select_persisted_features(df, features)
            X_scaled = scaler_X.transform(X_raw)
        except Exception as exc:
            self.log.error(f"训练/推理特征契约不一致，拒绝预测: {exc}")
            return None
        X_seq = np.asarray([X_scaled[i - self.window:i] for i in range(self.window, len(df))], dtype=np.float32)
        if X_seq.size == 0:
            self.log.warning("X_seq 为空，跳过预测")
            return None
        y_raw = df[['close']].values
        y_scaled = scaler_y.transform(y_raw)
        y_seq_scaled = y_scaled[self.window:].astype(np.float32)

        # ---- 取本地 Coinglass 快照 + 完整度评估，用于因子融合 ----
        try:
            market_snapshot = self.fetcher.build_local_snapshot(self.sym)
        except Exception:
            market_snapshot = {}
        try:
            base = self.sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
            completeness = assess_context_completeness(
                self.fetcher.metrics_dir,
                base,
                data_dir=getattr(self.fetcher, "db_dir", None),
            )
        except Exception:
            completeness = {"score": 0.0, "sources": {}, "missing": [], "generated_at": _now_iso()}

        # ---- LLM 辅助预测（可选，失败回退中性） ----
        openai_payload = None
        if self.llm_aux is not None:
            try:
                openai_payload = self.llm_aux.predict(
                    symbol=self.sym,
                    mode=self.mode,
                    snapshot=market_snapshot,
                    completeness=completeness,
                    data_sources_generated_at=self._collect_source_times(),
                )
            except Exception as exc:
                self.log.debug(f"LLM 辅助预测失败: {exc}")

        kline_last_price = float(df["close"].iloc[-1])
        current_meta = _fresh_liq_current_price(self.sym)
        current_price_for_metrics = float(current_meta.get("current_price") or kline_last_price)
        if current_meta.get("current_price_warning"):
            self.log.warning(f"当前价来源告警: {current_meta.get('current_price_warning')}")

        if current_meta.get("current_price_warning") and current_meta.get("current_price") is None:
            self.log.warning(f"当前价不可用，回退K线收盘价: {current_meta.get('current_price_warning')}")

        loaded_model_metadata: Dict[str, Any] = {}
        try:
            metadata_path = self.results_dir / f"{self.sym}_{self.mode}_training.json"
            if metadata_path.exists():
                import json as _json
                candidate = _json.loads(metadata_path.read_text(encoding="utf-8"))
                if isinstance(candidate, dict):
                    loaded_model_metadata = candidate
        except Exception as exc:
            self.log.warning("模型训练元数据不可读，LSTM 仅作为未验证展示输出: %s", exc)

        try:
            # The external PIT panel must be queried at the exact cutoff used
            # by the new Alpha.  The legacy Binance frame may close at a
            # different instant and is only a display/baseline input here.
            panel_price_frame = (
                alpha_price_frame if not alpha_price_frame.empty else df
            )
            panel_as_of = price_frame_cutoff(panel_price_frame)
            external_panel_context = self.fetcher.get_external_panel_context(
                as_of=panel_as_of
            )
        except Exception as exc:
            external_panel_context = {
                "status": "outage",
                "source": "trad_data_service.canonical_panel",
                "data": None,
                "warnings": [],
                "error": f"{type(exc).__name__}: {exc}",
            }

        return {
            "sym": self.sym,
            "tf_code": self.tf_code,
            "mode": self.mode,
            "cfg": self.cfg,
            "brain_df": df.tail(max(self.window + 32, 360)).copy(),
            "alpha_price_frame": alpha_price_frame.tail(1_000).copy(),
            "alpha_price_source": alpha_price_frame.attrs.get("data_source"),
            "alpha_price_error": alpha_price_error,
            "model_path_str": str(self.model_path),
            "scaler_path_str": str(self.scaler_path),
            "X_seq": X_seq,
            "y_seq_scaled": y_seq_scaled,
            "last_price": current_price_for_metrics,
            "kline_last_price": kline_last_price,
            "current_price": current_price_for_metrics,
            "current_price_source": current_meta.get("current_price_source"),
            "current_price_mtime": current_meta.get("current_price_mtime"),
            "current_price_age_seconds": current_meta.get("current_price_age_seconds"),
            "current_price_warning": current_meta.get("current_price_warning"),
            "feature_columns": features,
            "market_snapshot": market_snapshot,
            "context_completeness": completeness,
            "openai_prediction": openai_payload,
            # Calibration is applied by Portfolio only after the worker has
            # produced the real model return.  A proxy factor must never be
            # presented as calibration of the model output.
            "online_calibration": None,
            "loaded_model_metadata": loaded_model_metadata,
            "data_sources_generated_at": self._collect_source_times(),
            "external_panel_context": external_panel_context,
            # 行情数据源溯源（由 data_fetch.get_ohlcv 写入 df.attrs）
            "market_data_source": df.attrs.get("data_source"),
            "data_source_status": df.attrs.get("source_status"),
            "latest_kline_ts": df.attrs.get("latest_kline_ts"),
            "market_data_fetched_at": df.attrs.get("fetched_at"),
            "market_data_new_candles": df.attrs.get("new_candles"),
        }

    def _collect_source_times(self) -> Dict[str, Any]:
        """汇总各 Coinglass 数据源的 generated_at 时间，便于前端展示。"""
        try:
            base = self.sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
            metrics_dir = self.fetcher.metrics_dir
        except Exception:
            return {}
        out: Dict[str, Any] = {}
        files = [
            ("liqmap", self.fetcher.db_dir / f"{base}.json"),
            ("open_interest", metrics_dir / f"{base}_open_interest.json"),
            ("funding_rate", metrics_dir / f"{base}_funding_rate.json"),
            ("long_short_ratio", metrics_dir / f"{base}_long_short_ratio.json"),
            ("volume_24h", metrics_dir / f"{base}_volume_24h.json"),
            ("liquidation_today", metrics_dir / f"{base}_liquidation_today.json"),
            ("events", metrics_dir / "events.json"),
            ("news_context", metrics_dir / "news_context.json"),
            ("financial_calendar", metrics_dir / "financial_calendar.json"),
            ("whale_alert", metrics_dir / "whale_alert.json"),
            ("fear_greed_index", metrics_dir / "fear_greed_index.json"),
        ]
        import json as _json
        for name, path in files:
            try:
                if not path.exists():
                    continue
                with path.open("r", encoding="utf-8") as f:
                    data = _json.load(f)
                if isinstance(data, dict):
                    out[name] = data.get("generated_at") or data.get("ts")
            except Exception:
                continue
        return out
