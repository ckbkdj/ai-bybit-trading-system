import json
import sys
from asyncio import sleep
from datetime import datetime, timezone
from pathlib import Path

import asyncio
import ccxt.async_support as ccxt
import logging
import pandas as pd
import sqlite3
import time
from typing import Any, Dict, Optional

from .http_client import HTTPClient
from .market_context import build_market_feature_snapshot

BINANCE = "https://fapi.binance.com"
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class MarketDataUnavailable(RuntimeError):
    """实时市场数据不可用（如 Binance 451 受限或缓存过期）。

    生产环境中应当向上游传播该异常，让调度器跳过本轮预测/训练，
    而不是用过期缓存伪造一份“正常”的预测结果。属性：

    - ``symbol`` / ``timeframe``：触发的标的与周期
    - ``source``：数据源名（如 ``binance_futures``）
    - ``status``：``restricted_location`` / ``stale_cache`` / ``fetch_failed`` / ``empty``
    - ``reason``：人类可读原因
    - ``latest_ts``：本地缓存最新 K 线时间（如有）
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        *,
        source: str = "binance_futures",
        status: str = "unavailable",
        reason: str = "",
        latest_ts: Optional[str] = None,
    ) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.source = source
        self.status = status
        self.reason = reason or status
        self.latest_ts = latest_ts
        msg = (
            f"MarketDataUnavailable[{source}] {symbol}-{timeframe}: "
            f"{status} ({self.reason}); latest_ts={latest_ts}"
        )
        super().__init__(msg)


# Timeframe-aware max age between "now" and the latest cached K 线. 若实际 latest
# 比这更旧，且本轮抓取没有新增 K 线，则视为数据源不可用，禁止用陈旧缓存出预测。
_TF_MAX_KLINE_AGE: Dict[str, pd.Timedelta] = {
    "1m": pd.Timedelta(minutes=5),
    "3m": pd.Timedelta(minutes=10),
    "5m": pd.Timedelta(minutes=15),
    "15m": pd.Timedelta(minutes=45),
    "30m": pd.Timedelta(minutes=90),
    "1h": pd.Timedelta(hours=2),
    "2h": pd.Timedelta(hours=4),
    "4h": pd.Timedelta(hours=8),
    "6h": pd.Timedelta(hours=12),
    "8h": pd.Timedelta(hours=16),
    "12h": pd.Timedelta(hours=20),
    "1d": pd.Timedelta(hours=36),
}


def _max_kline_age(tf: str) -> pd.Timedelta:
    return _TF_MAX_KLINE_AGE.get(tf, pd.Timedelta(hours=8))


def _timeframe_ms(tf: str) -> int:
    unit = str(tf)[-1].lower()
    n = int(str(tf)[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    if unit not in mult:
        raise ValueError(f"unsupported timeframe: {tf}")
    return n * mult[unit]


def _now_utc() -> pd.Timestamp:
    return pd.Timestamp.utcnow().tz_localize(None)


def _is_binance_451(err: BaseException) -> bool:
    """识别 Binance / CCXT 抛出的 HTTP 451 受限地域错误。

    Binance 在被限制地区返回的 HTTP 451 通常会被 ccxt 包成 ExchangeNotAvailable /
    NetworkError / DDoSProtection 等异常，但错误文本里会带 451 或
    'restricted location' 字样。这里做宽松匹配，避免重复 sleep(5) 拖慢消费者。
    """
    try:
        s = str(err)
    except Exception:
        return False
    s_low = s.lower()
    if " 451 " in f" {s} " or s_low.startswith("451 ") or "http 451" in s_low:
        return True
    if "451" in s and ("restricted" in s_low or "eligibility" in s_low or "location" in s_low):
        return True
    if "restricted location" in s_low:
        return True
    return False
# ssl_context = ssl.create_default_context()
# ssl_context.check_hostname = False
# ssl_context.verify_mode = ssl.CERT_NONE
class DataFetcher:
    """异步行情 + 指标数据拉取 & 本地缓存"""
    def __init__(self, cfg: dict, http: HTTPClient):
        self.cfg, self.http = cfg, http
        self.log = logging.getLogger("Fetcher")
        self.db_dir = Path(cfg["general"]["db_dir"]).expanduser()
        self.db_dir.mkdir(exist_ok=True)
        self.cache_days = cfg["general"]["cache_days"]
        proxies = cfg["api"].get("proxies",{})
        http_proxy = proxies.get("http", "")
        https_proxy = proxies.get("https", "")
        aiohttp_proxy = https_proxy or http_proxy
        self.exchange = ccxt.binanceusdm({
            "enableRateLimit": True,
            "options": {"adjustForTimeDifference": True},
            "aiohttp_proxy": aiohttp_proxy,
            'timeout': 30000
        })

        # ---------- SQLite 缓存 ----------
    def _db(self, sym): return self.db_dir / f"{sym}.sqlite"
    def _table(self, tf): return f"k_{tf}"

    def _load_cache(self, sym, tf):
        p = self._db(sym)
        if not p.exists(): return pd.DataFrame()
        with sqlite3.connect(p) as c:
            try:
                return pd.read_sql(f"SELECT * FROM {self._table(tf)}", c, parse_dates=["ts"])
            except Exception: return pd.DataFrame()

    def _save_cache(self, sym, tf, df):
        with sqlite3.connect(self._db(sym)) as c:
            df.to_sql(self._table(tf), c, if_exists="replace", index=False)

    # ---------- Binance OHLCV ----------
    async def get_ohlcv(self, sym, tf, limit):
        self.log.debug(f"Starting get_ohlcv for {sym}-{tf}, limit: {limit}")
        df_cache = self._load_cache(sym, tf)

        # 1. 裁剪旧缓存：只保留 cache_days 内的数据，避免缓存无限增长
        if not df_cache.empty:
            cutoff_cache_days = df_cache["ts"].max() - pd.Timedelta(days=self.cache_days)
            df_cache = df_cache[df_cache["ts"] >= cutoff_cache_days]
            self.log.debug(f"Cache for {sym}-{tf} trimmed to {len(df_cache)} rows within {self.cache_days} days.")

        new_data_rows = []
        old_data_rows = []
        latest_fetch_ok = False

        # 2. 尝试获取最新的数据来更新缓存 (优先补齐最新部分)
        # 生产规则：最新一轮抓取必须成功，否则不允许用陈旧缓存继续出预测。
        try:
            # Binance合约K线单次最多1500
            latest_raw_data = await self.exchange.fapiPublicGetKlines(
                {"symbol": sym, "interval": tf, "limit": 1500}
            )
            latest_fetch_ok = True
            if latest_raw_data:
                if not df_cache.empty:
                    latest_ts_cache = df_cache["ts"].max().timestamp() * 1000
                    # 筛选出比缓存中最新时间戳更晚的K线
                    newly_fetched_candles = [
                        row for row in latest_raw_data if int(row[0]) > latest_ts_cache
                    ]
                    if newly_fetched_candles:
                        new_data_rows = newly_fetched_candles
                        self.log.warning(f"Fetched {len(new_data_rows)} NEW candles for {sym}-{tf}.")
                    else:
                        self.log.debug(f"No truly new candles found via latest batch for {sym}-{tf}.")
                else:  # 缓存为空，所有获取到的最新数据都是新的
                    new_data_rows = latest_raw_data
                    self.log.warning(
                        f"Cache was empty. Fetched {len(new_data_rows)} initial NEW candles for {sym}-{tf}.")
            else:
                self.log.warning(f"Could not fetch any latest data for {sym}-{tf}. API might be returning empty.")
        except Exception as e:
            cache_latest = (
                df_cache["ts"].max().isoformat() if not df_cache.empty else None
            )
            if _is_binance_451(e):
                # 受限地域：必须向上游报错，不再静默走缓存。
                self.log.error(
                    f"Binance 451 restricted for {sym}-{tf} latest fetch; refusing stale cache fallback."
                )
                raise MarketDataUnavailable(
                    sym, tf,
                    source="binance_futures",
                    status="restricted_location",
                    reason=f"binance 451: {e}",
                    latest_ts=cache_latest,
                ) from e
            # 其他失败：同样视为不可用，避免用旧缓存伪造实时预测
            self.log.error(f"Error fetching latest data for {sym}-{tf}: {e}")
            raise MarketDataUnavailable(
                sym, tf,
                source="binance_futures",
                status="fetch_failed",
                reason=f"{type(e).__name__}: {e}",
                latest_ts=cache_latest,
            ) from e

        # 3. 合并新获取的数据和现有缓存
        df_combined_after_new = df_cache.copy()  # Start with existing cache
        if new_data_rows:
            new_df = pd.DataFrame(new_data_rows, columns=[
                "ts", "open", "high", "low", "close", "volume", "_1", "_2", "_3", "_4", "_5", "_6"])
            new_df = new_df[["ts", "open", "high", "low", "close", "volume"]]
            new_df["ts"] = pd.to_datetime(new_df["ts"].astype(int), unit="ms")

            # 使用concat合并并去重排序，确保新数据在后面
            df_combined_after_new = pd.concat([df_combined_after_new, new_df]).drop_duplicates("ts").sort_values("ts")
            self.log.debug(f"Combined after new fetch: {len(df_combined_after_new)} rows.")
        else:
            self.log.debug(
                f"No new data fetched. Current combined data: {len(df_combined_after_new)} rows (from cache).")

        # 4. 如果总数据量不足 limit，则向过去拉取旧数据
        miss_for_limit = limit - len(df_combined_after_new)
        if miss_for_limit > 0:
            self.log.debug(f"Still missing {miss_for_limit} candles for limit. Fetching OLD data.")
            # 从当前最早的K线时间点开始，向前获取数据
            end_ms_for_old = int(
                df_combined_after_new["ts"].min().timestamp() * 1000) - 1 if not df_combined_after_new.empty else None

            # 循环获取直到达到 limit 或没有更多历史数据
            while miss_for_limit > 0:
                batch = min(1500, miss_for_limit)  # 每次最多拉取1500条
                params = {"symbol": sym, "interval": tf, "limit": batch}
                if end_ms_for_old is not None:
                    params["endTime"] = end_ms_for_old

                try:
                    data = await self.exchange.fapiPublicGetKlines(params)
                    if not data:
                        self.log.warning(f"未能为 {sym}-{tf} 获取到更多旧 K线数据. 可能已达到数据尽头或接口限制.")
                        break  # 没有更多数据可获取，退出循环

                    self.log.warning(f"获取 {sym}-{tf} 剩余数量： {miss_for_limit} 批次数据量: {len(data)}")
                    old_data_rows = data + old_data_rows  # 前插，保持时间顺序

                    end_ms_for_old = int(data[0][0]) - 1  # 更新end为本批最旧K线的前一毫秒
                    miss_for_limit -= len(data)
                except Exception as e:
                    if _is_binance_451(e):
                        self.log.warning(
                            f"Binance 451 restricted for {sym}-{tf} old-data fetch; stop backfill, rely on cache."
                        )
                        break  # 受限地域：跳出循环，避免重复 sleep
                    self.log.error(f"Error fetching old data for {sym}-{tf}: {e}")
                    await sleep(5)  # 发生错误时等待，然后跳出循环避免卡死
                    break

        # 5. 合并所有数据 (缓存 + 新获取的 + 旧获取的)
        df_final_raw = df_combined_after_new.copy()  # Start with data combined after new fetch
        if old_data_rows:
            old_df = pd.DataFrame(old_data_rows, columns=[
                "ts", "open", "high", "low", "close", "volume", "_1", "_2", "_3", "_4", "_5", "_6"])
            old_df = old_df[["ts", "open", "high", "low", "close", "volume"]]
            old_df["ts"] = pd.to_datetime(old_df["ts"].astype(int), unit="ms")

            # 将旧数据合并到现有数据的前面
            df_final_raw = pd.concat([old_df, df_final_raw]).drop_duplicates("ts").sort_values("ts")
            self.log.debug(f"Combined all data (cache + new + old): {len(df_final_raw)} rows.")

        # 6. 最终统一裁剪策略：优先满足 limit 数量，然后裁剪到 cache_days 以便保存
        # 裁剪到 limit (从最新开始保留 limit 条)
        if len(df_final_raw) > limit:
            df_to_return = df_final_raw.tail(limit)
            self.log.debug(f"Final data trimmed by result limit ({limit}) to {len(df_to_return)} rows.")
        else:
            df_to_return = df_final_raw.copy()  # 如果不足 limit，则全部保留
            self.log.debug(f"Final data not enough for limit, keeping all {len(df_to_return)} rows.")

        # 7. 保存数据到缓存 (按 cache_days 裁剪，为了防止缓存文件过大)
        if not df_to_return.empty:
            # 缓存裁剪应基于 df_to_return 的最大时间，确保存储的数据是最新的 N 天
            cache_cutoff_by_days = df_to_return["ts"].max() - pd.Timedelta(days=self.cache_days)
            df_to_save = df_to_return[df_to_return["ts"] >= cache_cutoff_by_days]
            self.log.debug(f"Data to save trimmed by cache_days ({self.cache_days}) to {len(df_to_save)} rows.")
            self._save_cache(sym, tf, df_to_save)
        else:
            self.log.warning(f"No data to save for {sym}-{tf} after final trim. Clearing old cache.")
            self._save_cache(sym, tf, pd.DataFrame())  # 即使空也保存，清空旧缓存

        self.log.warning(f"返回k数据 - sym {sym}-{tf} limit:{limit} 最终K线数据量: {len(df_to_return)}.")

        # 8. 数据源时效性硬校验：若整体 K 线已经过陈旧，禁止用于实时预测。
        fetched_at = datetime.now(timezone.utc).astimezone().isoformat()
        latest_kline_ts_iso: Optional[str] = None
        if not df_to_return.empty:
            latest_ts = df_to_return["ts"].max()
            latest_kline_ts_iso = latest_ts.isoformat()
            age = _now_utc() - latest_ts
            max_age = _max_kline_age(tf)
            # 若本轮未拉到新 K 线且缓存已经超过容忍时间，标记为不可用。
            if not new_data_rows and age > max_age:
                self.log.error(
                    f"{sym}-{tf} latest fetch returned no new candles and cache is stale "
                    f"(latest={latest_kline_ts_iso}, age={age}, max_age={max_age}); "
                    f"refusing to serve stale data."
                )
                raise MarketDataUnavailable(
                    sym, tf,
                    source="binance_futures",
                    status="stale_cache",
                    reason=(
                        f"latest cache {latest_kline_ts_iso} older than allowed {max_age} for {tf}"
                    ),
                    latest_ts=latest_kline_ts_iso,
                )
        else:
            # 完全没有数据可用
            raise MarketDataUnavailable(
                sym, tf,
                source="binance_futures",
                status="empty",
                reason="no candles available after fetch+cache merge",
                latest_ts=None,
            )

        # 标注数据源元信息，供推理/训练/前端可见溯源。
        try:
            df_to_return.attrs.update({
                "data_source": "binance_futures",
                "source_status": "ok",
                "latest_kline_ts": latest_kline_ts_iso,
                "fetched_at": fetched_at,
                "latest_fetch_ok": bool(latest_fetch_ok),
                "new_candles": int(len(new_data_rows)),
                "timeframe": tf,
                "symbol": sym,
            })
        except Exception:
            pass

        return df_to_return
    async def get_ohlcv_incremental(self, sym, tf, since_open_time_ms, limit=1500):
        """Fetch only candles with open_time >= since_open_time_ms.

        Used by the incremental feature store after raw_kline has been initialized.
        It deliberately does not backfill the configured 3-year limit.
        """
        params = {"symbol": sym, "interval": tf, "limit": min(int(limit), 1500)}
        if since_open_time_ms is not None:
            params["startTime"] = int(since_open_time_ms)
        try:
            data = await self.exchange.fapiPublicGetKlines(params)
        except Exception as e:
            if _is_binance_451(e):
                raise MarketDataUnavailable(sym, tf, source="binance_futures", status="restricted_location", reason=f"binance 451: {e}") from e
            raise MarketDataUnavailable(sym, tf, source="binance_futures", status="fetch_failed", reason=f"{type(e).__name__}: {e}") from e
        if not data:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        tf_ms = _timeframe_ms(tf)
        rows = []
        for row in data:
            open_ms = int(row[0])
            close_ms = open_ms + int(tf_ms)
            # Exclude an unfinished current candle; training/prediction must use closed Klines only.
            if close_ms > now_ms:
                continue
            if since_open_time_ms is not None and open_ms < int(since_open_time_ms):
                continue
            rows.append(row)
        if not rows:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "_1", "_2", "_3", "_4", "_5", "_6"])
        df = df[["ts", "open", "high", "low", "close", "volume"]]
        df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms", utc=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)

    # ---------- Funding & Long/Short ----------
    async def funding_rate(self, sym):
        try:
            base, quote = sym[:-4], sym[-4:]
            symbol = f"{base}/{quote}"
            res = await self.exchange.fetch_funding_rate(symbol)
            return float(res.get("fundingRate", 0.0))
        except Exception as e:
            if _is_binance_451(e):
                # 生产规则：受限地域不得用 0.0 中性值伪造，必须报错让上游跳过本轮预测。
                self.log.error(f"funding_rate {sym}: Binance 451 restricted; raising MarketDataUnavailable.")
                raise MarketDataUnavailable(
                    sym, "funding_rate",
                    source="binance_futures",
                    status="restricted_location",
                    reason=f"binance 451: {e}",
                ) from e
            # 若CCXT不支持，则回退用HTTP
            try:
                j = await self.http.get(f"{BINANCE}/fapi/v1/fundingRate", {"symbol": sym, "limit": 1})
                if isinstance(j, dict) and j.get("_http_status") == 451:
                    raise MarketDataUnavailable(
                        sym, "funding_rate",
                        source="binance_futures",
                        status="restricted_location",
                        reason="HTTP 451 from fapi/v1/fundingRate",
                    )
                if j is None:
                    raise MarketDataUnavailable(
                        sym, "funding_rate",
                        source="binance_futures",
                        status="fetch_failed",
                        reason="HTTP fallback returned None",
                    )
                try:
                    return float(j[-1]["fundingRate"])
                except Exception as e3:
                    raise MarketDataUnavailable(
                        sym, "funding_rate",
                        source="binance_futures",
                        status="fetch_failed",
                        reason=f"parse error: {e3}",
                    )
            except MarketDataUnavailable:
                raise
            except Exception as e2:
                if _is_binance_451(e2):
                    raise MarketDataUnavailable(
                        sym, "funding_rate",
                        source="binance_futures",
                        status="restricted_location",
                        reason=f"binance 451 (http fallback): {e2}",
                    ) from e2
                raise MarketDataUnavailable(
                    sym, "funding_rate",
                    source="binance_futures",
                    status="fetch_failed",
                    reason=f"{type(e2).__name__}: {e2}",
                ) from e2

    async def long_short_ratio(self, sym, period="2h"):
        try:
            j = await self.http.get(f"{BINANCE}/futures/data/globalLongShortAccountRatio",
                                    {"symbol": sym, "period": period, "limit": 1})
        except Exception as e:
            if _is_binance_451(e):
                raise MarketDataUnavailable(
                    sym, f"long_short_ratio:{period}",
                    source="binance_futures",
                    status="restricted_location",
                    reason=f"binance 451: {e}",
                ) from e
            raise MarketDataUnavailable(
                sym, f"long_short_ratio:{period}",
                source="binance_futures",
                status="fetch_failed",
                reason=f"{type(e).__name__}: {e}",
            ) from e
        if isinstance(j, dict) and j.get("_http_status") == 451:
            raise MarketDataUnavailable(
                sym, f"long_short_ratio:{period}",
                source="binance_futures",
                status="restricted_location",
                reason="HTTP 451 from globalLongShortAccountRatio",
            )
        if j is None:
            raise MarketDataUnavailable(
                sym, f"long_short_ratio:{period}",
                source="binance_futures",
                status="fetch_failed",
                reason="HTTP returned None",
            )
        try:
            return float(j[-1]["longShortRatio"])
        except Exception as e3:
            raise MarketDataUnavailable(
                sym, f"long_short_ratio:{period}",
                source="binance_futures",
                status="fetch_failed",
                reason=f"parse error: {e3}",
            )

    # ---------- 本地 Coinglass 指标读取（liqmap_fetcher 的副产物） ----------
    @property
    def metrics_dir(self) -> Path:
        """data/coinglass_metrics 目录（liqmap_fetcher.py 落盘）。"""
        return self.db_dir / "coinglass_metrics"

    def _liqmap_payload(self, sym: str) -> Optional[Dict[str, Any]]:
        """读取本地爆仓图 JSON（``data/{BASE}.json``）。"""
        base = sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
        path = self.db_dir / f"{base}.json"
        try:
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return None
        return None

    def load_local_metric(self, base: str, metric: str) -> Optional[Dict[str, Any]]:
        """读取单个 ``data/coinglass_metrics/{base}_{metric}.json`` wrapper。"""
        try:
            path = self.metrics_dir / f"{base}_{metric}.json"
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def load_local_metrics(self, base: str) -> Dict[str, Any]:
        """读取该币种全部本地 Coinglass wrappers。"""
        out: Dict[str, Any] = {}
        if not self.metrics_dir.exists():
            return out
        for path in self.metrics_dir.glob(f"{base}_*.json"):
            try:
                with path.open("r", encoding="utf-8") as f:
                    out[path.stem.replace(f"{base}_", "")] = json.load(f)
            except Exception:
                continue
        return out

    def build_local_snapshot(self, sym: str) -> Dict[str, float]:
        """组合本地爆仓图 + Coinglass 指标 + news_context 为统一特征快照。"""
        base = sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
        return build_market_feature_snapshot(
            liqmap_payload=self._liqmap_payload(sym),
            metrics_dir=self.metrics_dir,
            base=base,
        )

    async def get_market_snapshot(self, sym: str) -> Dict[str, float]:
        """获取市场快照：本地 Coinglass 优先，缺失字段再用 Binance 兜底。"""
        snap: Dict[str, float] = self.build_local_snapshot(sym)

        # --- Binance fallback：仅对仍为 0 的关键字段补一刀 ---
        if snap.get("funding_rate", 0.0) == 0.0:
            try:
                snap["funding_rate"] = await self.funding_rate(sym)
            except Exception:
                pass
        if snap.get("long_short_ratio", 0.0) == 0.0:
            try:
                snap["long_short_ratio"] = await self.long_short_ratio(sym, "2h")
            except Exception:
                pass
        if snap.get("volume_24h", 0.0) == 0.0:
            try:
                base, quote = sym[:-4], sym[-4:]
                ticker = await self.exchange.fetch_ticker(f"{base}/{quote}")
                snap["volume_24h"] = float(ticker.get("quoteVolume") or 0.0)
            except Exception:
                pass
        return snap

    async def close(self):
        """Close the CCXT client session."""
        if self.exchange:
            await self.exchange.close()