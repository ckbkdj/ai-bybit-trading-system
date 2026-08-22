import asyncio
import json
import logging
import os
import pickle
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple, Dict

# ---------------------------------------------------------------------------
# 重要：必须在 `import keras` / `import tensorflow` 之前把线程上限 / 后端 /
# GPU 内存增长等环境变量设置好，否则 keras / TF 在 import 时会按默认值固化
# 线程池，再调用 tf.config.threading.set_*_parallelism_threads() 也无法生效。
# ---------------------------------------------------------------------------
if os.environ.get("AI_BOT_FORCE_CPU") == "1":
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
os.environ.setdefault("KERAS_BACKEND", "tensorflow")  # keras 3.x 后推荐
# CPU 线程上限（防止 BLAS/MKL/OpenBLAS/NumExpr/TF 各自抢满所有核心）。
os.environ.setdefault("OMP_NUM_THREADS", "2")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("TF_NUM_INTRAOP_THREADS", "2")
os.environ.setdefault("TF_NUM_INTEROP_THREADS", "1")
# 默认开启 GPU 内存按需增长，避免 TF 在 init 时一次性抢占显存。
os.environ.setdefault("TF_FORCE_GPU_ALLOW_GROWTH", "true")

import keras
import numpy as np
import pandas as pd
import talib
from sklearn.preprocessing import MinMaxScaler

import fcntl

from .market_context import (
    MARKET_FEATURE_COLUMNS,
    NEWS_FEATURE_COLUMNS,
)
from .data_fetch import MarketDataUnavailable
from .evaluation.time_series_split import purged_holdout_boundary


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _safe_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except Exception:
        return default

def atomic_save_model(model, model_path):
    model_path = Path(model_path)
    # 临时和备份都要 .keras 结尾
    tmp_path = model_path.with_name(model_path.stem + '.tmp.keras')
    bak_path = model_path.with_name(model_path.stem + '.bak1.keras')
    bak2_path = model_path.with_name(model_path.stem + '.bak2.keras')
    lock_path = str(model_path) + ".lock"
    with open(lock_path, "w") as lockfile:
        fcntl.flock(lockfile, fcntl.LOCK_EX)
        if bak_path.exists():
            bak_path.replace(bak2_path)
        if model_path.exists():
            model_path.replace(bak_path)
        model.save(tmp_path, include_optimizer=True)
        tmp_path.replace(model_path)
        fcntl.flock(lockfile, fcntl.LOCK_UN)

def run_training_in_process(prepared_data: Dict[str, Any]):
    sym = prepared_data['sym']
    tf_code = prepared_data['tf_code']
    log = logging.getLogger(f"Worker.Trainer.{sym}.{tf_code}")
    try:
        import tensorflow as tf


        model_path_str = prepared_data['model_path_str']
        scaler_path_str = prepared_data['scaler_path_str']
        X_scaled = prepared_data['X_scaled']
        y_scaled = prepared_data['y_scaled']
        window = prepared_data['window']
        batch = prepared_data['batch']
        epochs = prepared_data['epochs']
        fit_needed = prepared_data['fit_needed']
        scaler_X = prepared_data['scaler_X']
        scaler_y = prepared_data['scaler_y']


        log.info("子进程已启动，开始执行 TensorFlow 训练。")

        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
        if os.environ.get("AI_BOT_FORCE_CPU") == "1":
            os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
        try:
            gpus = tf.config.experimental.list_physical_devices('GPU')
            if gpus:
                for gpu in gpus:
                    try:
                        tf.config.experimental.set_memory_growth(gpu, True)
                    except RuntimeError as _e:
                        # 若 TF 已初始化，set_memory_growth 会抛出 RuntimeError；忽略并继续。
                        log.warning(f"set_memory_growth 已无法修改 (TF 已初始化): {_e}")
                log.info(f"TensorFlow GPU devices enabled: {gpus}")
            else:
                log.warning("TensorFlow GPU devices not visible; running on CPU")
        except RuntimeError as e:
            log.error(f"设置GPU内存增长失败: {e}")
        # 从环境变量读取线程上限，便于在 run_v3.sh 中统一管理；
        # 若 TF 已经初始化，set_*_parallelism_threads 会抛 RuntimeError，安全忽略。
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

        def build_lstm_functional(shape: Tuple[int, int]) -> keras.Model:
            from keras import layers, Model
            inputs = layers.Input(shape=shape, name="input")
            x = layers.LSTM(64, return_sequences=True)(inputs)
            x = layers.LSTM(32)(x)
            outputs = layers.Dense(1, name="output")(x)
            model = Model(inputs=inputs, outputs=outputs, name="LSTM_Model")
            model.compile(optimizer="adam", loss="mse")
            return model

        # ---- 训练开始时间记录（每模式训练时间元数据） ----
        training_started_at = _now_iso()
        training_started_ts = time.time()

        log.info(f"[{os.getpid()}] 检查原始数据...")
        # 检查 X_scaled
        if np.any(np.isnan(X_scaled)):
            log.error(f"[{os.getpid()}] 错误: X_scaled 中包含 NaN 值!")
            # 这里可以添加一些调试信息，比如打印有 NaN 的行
            nan_rows = np.where(np.isnan(X_scaled).any(axis=1))
            log.error(f"[{os.getpid()}] 包含NaN值的行索引: {nan_rows}")
            # 打印包含NaN的行数据
            log.error(f"[{os.getpid()}] 包含NaN值的行数据示例:\n{X_scaled[nan_rows[0][0]]}")

        if np.any(np.isinf(X_scaled)):
            log.error(f"[{os.getpid()}] 错误: X_scaled 中包含 Inf 值!")

        # 检查 y_scaled
        if np.any(np.isnan(y_scaled)):
            log.error(f"[{os.getpid()}] 错误: y_scaled 中包含 NaN 值!")

        if np.any(np.isinf(y_scaled)):
            log.error(f"[{os.getpid()}] 错误: y_scaled 中包含 Inf 值!")

        log.info(f"[{os.getpid()}] 原始数据检查完成。X_scaled 形状: {X_scaled.shape}, y_scaled 形状: {y_scaled.shape}")

        validation_fraction = float(prepared_data.get("validation_fraction", 0.2))
        requested_purge = int(prepared_data.get("validation_purge_bars", window))
        # A complete sequence window is purged between train and validation. This
        # deliberately gives up some data to ensure the holdout was never observed
        # by either model fitting or overlapping feature windows.
        boundary = purged_holdout_boundary(
            len(y_scaled),
            validation_fraction=validation_fraction,
            minimum_train_size=window + 2,
            minimum_validation_size=max(8, window // 4),
            purge_size=max(window, requested_purge),
        )
        train_ds = keras.preprocessing.timeseries_dataset_from_array(
            data=X_scaled[: boundary.train_end - 1],
            targets=y_scaled[window : boundary.train_end],
            sequence_length=window,
            batch_size=batch,
            shuffle=False,
        )
        validation_context_start = boundary.validation_start - window
        validation_ds = keras.preprocessing.timeseries_dataset_from_array(
            data=X_scaled[validation_context_start : boundary.validation_end - 1],
            targets=y_scaled[boundary.validation_start : boundary.validation_end],
            sequence_length=window,
            batch_size=batch,
            shuffle=False,
        )
        log.info(
            "Purged holdout prepared: train_end=%s validation_start=%s purge=%s",
            boundary.train_end,
            boundary.validation_start,
            boundary.purge_size,
        )
        input_shape = (window, X_scaled.shape[1])
        log.info("子进程已启动，input_shape")
        model_path = Path(model_path_str)
        log.info(f"子进程已启动，modelpath--{model_path_str}")
        # Never warm-start a validation candidate from a model that may have seen
        # the holdout in a previous training run.
        log.info("Building a fresh LSTM validation candidate")
        model = build_lstm_functional(input_shape)
        log.info("子进程已启动，load ok")
        model.summary(print_fn=log.info)
        log.info("子进程已启动，summary")
        callbacks = [
            keras.callbacks.EarlyStopping(
                monitor="val_loss",
                patience=int(prepared_data.get("early_stop_patience", 3)),
                restore_best_weights=True,
            ),
            keras.callbacks.ReduceLROnPlateau(
                monitor="val_loss",
                patience=int(prepared_data.get("reduce_lr_patience", 2)),
                factor=float(prepared_data.get("reduce_lr_factor", 0.5)),
                min_lr=1e-6,
            ),
        ]
        history = model.fit(
            train_ds,
            validation_data=validation_ds,
            epochs=epochs,
            callbacks=callbacks,
            verbose=0,
        )
        log.info("子进程已启动，fit")
        # ---- 训练完成元数据返回（每模式训练时间 / 新闻训练摘要 / 校验指标） ----
        training_finished_at = _now_iso()
        training_duration_sec = float(time.time() - training_started_ts)
        try:
            pred_scaled = model.predict(validation_ds, verbose=0).reshape(-1, 1)
            target_scaled = y_scaled[boundary.validation_start : boundary.validation_end].reshape(-1, 1)
            previous_scaled = y_scaled[
                boundary.validation_start - 1 : boundary.validation_end - 1
            ].reshape(-1, 1)
            pred_price = scaler_y.inverse_transform(pred_scaled).reshape(-1)
            target_price = scaler_y.inverse_transform(target_scaled).reshape(-1)
            previous_price = scaler_y.inverse_transform(previous_scaled).reshape(-1)
            valid_price = np.abs(previous_price) > 1e-12
            predicted_return = np.zeros_like(pred_price)
            actual_return = np.zeros_like(target_price)
            predicted_return[valid_price] = (
                pred_price[valid_price] / previous_price[valid_price] - 1.0
            )
            actual_return[valid_price] = (
                target_price[valid_price] / previous_price[valid_price] - 1.0
            )
            rmse = float(np.sqrt(np.mean((predicted_return - actual_return) ** 2)))
            acc = float(np.mean(np.sign(predicted_return) == np.sign(actual_return)))
            price_rmse = float(np.sqrt(np.mean((pred_price - target_price) ** 2)))
        except Exception as exc:
            raise RuntimeError("purged holdout validation failed; candidate was not saved") from exc
        if not all(np.isfinite(value) for value in (rmse, acc, price_rmse)):
            raise RuntimeError("purged holdout metrics are non-finite; candidate was not saved")

        # Only a candidate with a successfully evaluated untouched holdout may
        # replace the active weak-prior model and scaler.
        log.info("子进程已启动，save start")
        atomic_save_model(model, model_path)
        log.info("子进程已启动，save finish")
        if fit_needed:
            feature_names = prepared_data.get("feature_columns")
            with open(scaler_path_str, "wb") as f:
                pickle.dump((scaler_X, scaler_y, feature_names), f)
                log.info(f"新的缩放器已保存到 {scaler_path_str}")
        log.info("子进程训练任务完成，即将退出。")

        meta = {
            "symbol": sym,
            "mode": prepared_data.get("mode"),
            "timeframe": tf_code,
            "model_version": "lstm_keras_v2_purged_holdout",
            "training_started_at": training_started_at,
            "training_finished_at": training_finished_at,
            "training_duration_sec": training_duration_sec,
            "training_mode_time": {
                "timeframe": tf_code,
                "window": int(window),
                "horizon": 1,
                "samples": int(prepared_data.get("samples", len(y_scaled))),
            },
            "training_data_policy": prepared_data.get("training_data_policy"),
            "anti_leakage": prepared_data.get("anti_leakage"),
            "requested_limit": prepared_data.get("requested_limit"),
            "actual_rows": prepared_data.get("actual_rows"),
            "data_start_ts": prepared_data.get("data_start_ts"),
            "data_end_ts": prepared_data.get("data_end_ts"),
            "kline_augmentation": prepared_data.get("kline_augmentation"),
            "validation_rmse_return": rmse,
            "validation_rmse_price": price_rmse,
            "validation_direction_acc": acc,
            "validation": {
                "kind": "purged_chronological_holdout",
                "train_rows": int(boundary.train_size),
                "validation_rows": int(boundary.validation_size),
                "purge_rows": int(boundary.purge_size),
                "validation_fraction": validation_fraction,
                "candidate_warm_started": False,
                "holdout_seen_during_fit": False,
                "best_validation_loss": float(min(history.history.get("val_loss", [0.0]))),
            },
            "feature_columns": prepared_data.get("feature_columns"),
            "news_training_summary": prepared_data.get("news_training_summary"),
        }

        # 写入 model_results/{SYMBOL}_{mode}_training.json，便于 API/前端展示
        try:
            mode = prepared_data.get("mode") or "default"
            results_dir = Path(prepared_data.get("results_dir") or "model_results")
            results_dir.mkdir(parents=True, exist_ok=True)
            path = results_dir / f"{sym}_{mode}_training.json"
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
            tmp.replace(path)
            log.info(f"训练元数据已保存: {path}")
        except Exception as exc:
            log.debug(f"训练元数据落盘失败: {exc}")
        return meta
    except Exception as e:
        # 在子进程中捕获并记录详细错误，这是关键！
        log.info(f"训练任务在子进程中失败: {e}")
        log.exception(f"训练任务在子进程中失败: {e}")
        # 重新抛出异常，以便父进程可以感知到
        raise
class TrainerDataPreparer:
    """训练数据准备器。

    本版本在原 keras LSTM 流程之上：
    * 引入本地 Coinglass 多维特征（资金费率 / 持仓 / 多空 / 爆仓压力 / 上下文新闻）
    * 注入新闻训练摘要 ``news_training_summary``，便于 API/前端展示新闻特征对训练的贡献
    * 训练元数据由 ``run_training_in_process`` 落盘到 ``model_results/{SYMBOL}_{mode}_training.json``
    * 保留旧的 16 列特征 (OHLCV + 技术指标 + funding + ls + news)，必要时扩展为更完整的多因子集
    """

    # 训练用扩展特征列：技术 + 主结构 + 新闻
    EXTRA_FEATURE_COLUMNS = MARKET_FEATURE_COLUMNS + NEWS_FEATURE_COLUMNS

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
        results_dir: Path | None = None,
        feature_store: Any | None = None,
        mode_spec: Any | None = None,
        built_dataset: Any | None = None,
    ):
        self.sym, self.tf_code = sym, tf_code
        self.limit, self.window = limits
        self.fetcher, self.sentiment = fetcher, sentiment
        self.cfg = cfg or {}
        self.mode = mode or tf_code
        self.results_dir = Path(results_dir or "model_results")
        self.feature_store = feature_store
        self.mode_spec = mode_spec
        self.built_dataset = built_dataset
        tag = f"{sym}_{tf_code}"
        self.model_path = model_dir / f"{tag}.keras"
        self.scaler_path = model_dir / f"{tag}_scaler.pkl"
        self.log = logging.getLogger(f"DataPrep.Trainer.{sym}.{tf_code}")

    async def _feature_df(self) -> pd.DataFrame:
        if self.built_dataset is not None:
            df = self.built_dataset.df.copy()
            if "open_time" in df.columns and "ts" not in df.columns:
                df["ts"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            # Keep legacy neutral placeholders; historical training remains Kline-only.
            df["funding_rate"], df["long_short_ratio"], df["news_sentiment"] = 0.0, 1.0, 0.0
            for col in ("ma", "rsi", "boll_upper", "boll_middle", "boll_lower", "macd", "macdsignal", "macdhist"):
                if col not in df.columns:
                    df[col] = 0.0
            return df
        try:
            df = await self.fetcher.get_ohlcv(self.sym, self.tf_code, self.limit)
        except MarketDataUnavailable as exc:
            self.log.error(
                f"训练行情数据不可用，跳过训练: {self.sym}-{self.tf_code} "
                f"source={exc.source} status={exc.status} reason={exc.reason} latest_ts={exc.latest_ts}"
            )
            raise
        if df.empty:
            return pd.DataFrame()
        src_attrs = dict(getattr(df, "attrs", {}) or {})
        df["ma"] = talib.SMA(df["close"], 14)
        df["rsi"] = talib.RSI(df["close"], 14)
        up, mid, low = talib.BBANDS(df["close"], 20)
        df[["boll_upper", "boll_middle", "boll_lower"]] = np.column_stack((up, mid, low))
        macd, sig, hist = talib.MACD(df["close"], 12, 26, 9)
        df[["macd", "macdsignal", "macdhist"]] = np.column_stack((macd, sig, hist))
        # 生产训练默认只吃历史 K 线/技术指标，禁止把“当前”资金费率、多空、新闻、爆仓快照广播到每根历史K线，避免未来函数。
        train_cfg = (self.cfg.get("training", {}) or {})
        historical_kline_only = bool(train_cfg.get("historical_kline_only", True))
        if historical_kline_only:
            df["funding_rate"], df["long_short_ratio"], df["news_sentiment"] = 0.0, 1.0, 0.0
        else:
            try:
                fr, ls, news = await asyncio.gather(
                    self.fetcher.funding_rate(self.sym),
                    self.fetcher.long_short_ratio(self.sym, "2h"),
                    self.sentiment.score(self.sym),
                )
            except MarketDataUnavailable as exc:
                self.log.error(
                    f"训练附加行情数据不可用，跳过训练: {self.sym}-{self.tf_code} "
                    f"source={exc.source} status={exc.status} reason={exc.reason}"
                )
                raise
            except Exception as exc:
                # 非核心 sentiment 等异常仍不伪造资金/多空；直接跳过本轮训练。
                self.log.error(f"训练附加特征获取失败，跳过训练: {exc}", exc_info=True)
                raise MarketDataUnavailable(
                    self.sym,
                    self.tf_code,
                    source="binance_futures",
                    status="fetch_failed",
                    reason=f"training feature gather error: {type(exc).__name__}: {exc}",
                ) from exc
            df["funding_rate"], df["long_short_ratio"], df["news_sentiment"] = fr, ls, news

        # ---- 注入本地 Coinglass 多维度特征 ----
        # 历史训练禁用当前快照广播；推理阶段仍可在 inferencer 中读取当前快照作为门控/融合。
        snap = {}
        if not historical_kline_only:
            try:
                snap = self.fetcher.build_local_snapshot(self.sym)
            except Exception:
                snap = {}
        for col in self.EXTRA_FEATURE_COLUMNS:
            if col in df.columns:
                continue
            df[col] = float(snap.get(col, 0.0))

        # ---- 纯 K 线增强特征：全部由历史 OHLCV/技术指标派生，不引入新闻/快照 ----
        close = pd.to_numeric(df["close"], errors="coerce").astype(float).replace(0, np.nan)
        open_ = pd.to_numeric(df["open"], errors="coerce").astype(float).replace(0, np.nan)
        high = pd.to_numeric(df["high"], errors="coerce").astype(float)
        low = pd.to_numeric(df["low"], errors="coerce").astype(float)
        volume = pd.to_numeric(df["volume"], errors="coerce").astype(float).fillna(0.0)
        df["ret_1"] = close.pct_change(1)
        df["ret_3"] = close.pct_change(3)
        df["ret_6"] = close.pct_change(6)
        df["logret_1"] = np.log(close / close.shift(1))
        df["range_pct"] = (high - low) / close
        df["body_pct"] = (close - open_) / open_
        df["upper_wick_pct"] = (high - np.maximum(open_, close)) / close
        df["lower_wick_pct"] = (np.minimum(open_, close) - low) / close
        df["volume_zscore"] = (volume - volume.rolling(48, min_periods=8).mean()) / (volume.rolling(48, min_periods=8).std() + 1e-9)
        df["atr_pct"] = talib.ATR(high.values.astype(float), low.values.astype(float), close.values.astype(float), 14) / close
        df["realized_vol_12"] = close.pct_change().rolling(12, min_periods=6).std()
        df["realized_vol_24"] = close.pct_change().rolling(24, min_periods=8).std()
        df["ema_gap_8_21"] = talib.EMA(close.values.astype(float), 8) / (talib.EMA(close.values.astype(float), 21) + 1e-9) - 1
        df["ma_gap_21_55"] = close.rolling(21, min_periods=8).mean() / (close.rolling(55, min_periods=16).mean() + 1e-9) - 1
        df["boll_pos"] = (close - df["boll_lower"]) / (df["boll_upper"] - df["boll_lower"] + 1e-9)
        df["trend_strength"] = close.pct_change(12) / (df["realized_vol_24"] + 1e-9)
        # 训练标签是下一根 close；这里整体 shift(1)，确保特征只使用上一根已收盘K线及以前信息。
        anti_shift = int(train_cfg.get("anti_leakage_shift_features", 1) or 0)
        kline_aug_cols = [
            "ret_1", "ret_3", "ret_6", "logret_1", "range_pct", "body_pct",
            "upper_wick_pct", "lower_wick_pct", "volume_zscore", "atr_pct",
            "realized_vol_12", "realized_vol_24", "ema_gap_8_21", "ma_gap_21_55",
            "boll_pos", "trend_strength",
        ]
        if anti_shift > 0:
            df[kline_aug_cols] = df[kline_aug_cols].shift(anti_shift)

        df.dropna(inplace=True)
        num_cols = df.select_dtypes(include=np.number).columns
        df[num_cols] = df[num_cols].astype(np.float32)
        try:
            df.attrs.update(src_attrs)
        except Exception:
            pass
        return df

    async def prepare_data_for_process(self, batch: int, epochs: int) -> Dict[str, Any] | None:
        train_cfg = (self.cfg.get("training", {}) or {})
        historical_kline_only = bool(train_cfg.get("historical_kline_only", True))
        try:
            df = await self._feature_df()
        except MarketDataUnavailable as exc:
            self.log.error(
                f"训练数据源不可用，本轮训练跳过: {self.sym}-{self.tf_code} "
                f"source={exc.source} status={exc.status} reason={exc.reason} latest_ts={exc.latest_ts}"
            )
            return None
        if df.empty or len(df) < self.window + 1:
            return None

        # 基础 16 列 + 扩展多因子（保留原列名，补 0 兼容旧 scaler）
        base_feats = [
            "open", "high", "low", "close", "volume",
            "ma", "rsi", "boll_upper", "boll_middle", "boll_lower",
            "macd", "macdsignal", "macdhist",
            "funding_rate", "long_short_ratio", "news_sentiment",
        ]
        if self.built_dataset is not None:
            feats = list(self.built_dataset.feature_columns)
        else:
            feats = list(base_feats) + [
                "ret_1", "ret_3", "ret_6", "logret_1", "range_pct", "body_pct",
                "upper_wick_pct", "lower_wick_pct", "volume_zscore", "atr_pct",
                "realized_vol_12", "realized_vol_24", "ema_gap_8_21", "ma_gap_21_55",
                "boll_pos", "trend_strength",
            ]

        scaler_X, scaler_y = None, None
        fit_needed = bool(train_cfg.get("strict_refit_scaler", True))
        prev_feature_names = None
        if self.scaler_path.exists() and not fit_needed:
            try:
                with open(self.scaler_path, "rb") as f:
                    bundle = pickle.load(f)
                if isinstance(bundle, tuple) and len(bundle) == 3:
                    scaler_X, scaler_y, prev_feature_names = bundle
                else:
                    scaler_X, scaler_y = bundle
            except Exception:
                scaler_X, scaler_y = MinMaxScaler(), MinMaxScaler()
                fit_needed = True
        if scaler_X is None or scaler_y is None:
            scaler_X, scaler_y = MinMaxScaler(), MinMaxScaler()
            fit_needed = True

        # 如果旧 scaler 列与当前列不一致，重新拟合
        if prev_feature_names is not None and list(prev_feature_names) != list(feats):
            self.log.info("特征列发生变化，重新拟合 scaler 以避免维度不兼容")
            scaler_X, scaler_y = MinMaxScaler(), MinMaxScaler()
            fit_needed = True

        X_raw, y_raw = df[feats].values, df[["close"]].values

        # Candidate scalers are fit only on the training partition. Reusing a
        # historical scaler is opt-in because its extrema may include the holdout.
        if fit_needed:
            validation_fraction = float(train_cfg.get("validation_fraction", 0.2))
            scaler_boundary = purged_holdout_boundary(
                len(X_raw),
                validation_fraction=validation_fraction,
                minimum_train_size=self.window + 2,
                minimum_validation_size=max(8, self.window // 4),
                purge_size=max(
                    self.window,
                    int(train_cfg.get("validation_purge_bars", self.window)),
                ),
            )
            scaler_X.fit(X_raw[: scaler_boundary.train_end])
            scaler_y.fit(y_raw[: scaler_boundary.train_end])
            X_scaled = scaler_X.transform(X_raw)
            y_scaled = scaler_y.transform(y_raw)
        else:
            X_scaled = scaler_X.transform(X_raw)
            y_scaled = scaler_y.transform(y_raw)

        # 新闻训练摘要：当前快照中各新闻因子的最新值与均值绝对值
        try:
            news_summary = {
                "enabled": False,
                "reason": "no_news_kline_only_training",
                "feature_columns": NEWS_FEATURE_COLUMNS,
                "feature_count": 0,
                "latest_values": {col: 0.0 for col in NEWS_FEATURE_COLUMNS},
                "mean_abs_values": {col: 0.0 for col in NEWS_FEATURE_COLUMNS},
                "weight_policy": "disabled_for_training_anti_leakage",
            }
        except Exception:
            news_summary = None

        return {
            "sym": self.sym, "tf_code": self.tf_code, "mode": self.mode,
            "model_path_str": str(self.model_path), "scaler_path_str": str(self.scaler_path),
            "X_scaled": X_scaled, "y_scaled": y_scaled,
            "window": self.window, "batch": batch, "epochs": epochs,
            "fit_needed": fit_needed, "scaler_X": scaler_X, "scaler_y": scaler_y,
            "validation_fraction": float(train_cfg.get("validation_fraction", 0.2)),
            "validation_purge_bars": int(train_cfg.get("validation_purge_bars", self.window)),
            "early_stop_patience": int(train_cfg.get("early_stop_patience", 3)),
            "reduce_lr_patience": int(train_cfg.get("reduce_lr_patience", 2)),
            "reduce_lr_factor": float(train_cfg.get("reduce_lr_factor", 0.5)),
            "feature_columns": feats,
            "samples": int(len(y_scaled)),
            "news_training_summary": news_summary,
            "training_data_policy": {
                "source": "ohlcv_kline_only",
                "no_news": True,
                "no_current_snapshot_broadcast": bool(historical_kline_only),
                "requested_history": "~3y_from_config_limit/cache_days",
            },
            "anti_leakage": {
                "feature_shift": int(train_cfg.get("anti_leakage_shift_features", 1) or 0),
                "target_alignment": "window_t_minus_1_to_t predicts next close via targets=y_scaled[window:]",
                "timeseries_shuffle": False,
                "chronological_validation": True,
            },
            "requested_limit": int(self.limit),
            "actual_rows": int(len(df)),
            "data_start_ts": str(df.index[0] if df.index.name else df["ts"].iloc[0]) if len(df) and "ts" in df.columns else None,
            "data_end_ts": str(df.index[-1] if df.index.name else df["ts"].iloc[-1]) if len(df) and "ts" in df.columns else None,
            "kline_augmentation": [c for c in feats if c not in base_feats],
            "results_dir": str(self.results_dir),
        }
