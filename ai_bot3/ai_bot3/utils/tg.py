import ccxt.async_support as ccxt
import asyncio
import pandas as pd
# ... other imports ...

BINANCE = "https://fapi.binance.com"

class DataFetcher:
    """Async market & indicator data fetcher with local caching."""
    def __init__(self, cfg: dict, http: HTTPClient):
        self.cfg = cfg
        self.http = http
        self.log = logging.getLogger("Fetcher")
        # Prepare database directory for caching
        self.db_dir = Path(cfg["general"]["db_dir"]).expanduser()
        self.db_dir.mkdir(exist_ok=True)
        self.cache_days = cfg["general"]["cache_days"]
        # Configure proxies for aiohttp (async) if provided
        proxies = cfg["api"].get("proxies", {})  # expecting keys "http" and/or "https"
        http_proxy = proxies.get("http", "")
        https_proxy = proxies.get("https", "")
        # Use https proxy if available, otherwise http proxy
        aiohttp_proxy = https_proxy or http_proxy
        if aiohttp_proxy:
            self.log.info(f"Using aiohttp proxy: {aiohttp_proxy}")
        # Initialize the CCXT Binance USD-M futures client with proper proxy and rate limit
        self.exchange = ccxt.binanceusdm({
            "enableRateLimit": True,
            "options": {"adjustForTimeDifference": True},
            "aiohttp_proxy": aiohttp_proxy,   # <-- Proper proxy config for async CCXT
            "timeout": 30000
        })

    # ... (cache helper methods unchanged) ...

    async def get_ohlcv(self, sym, tf, limit):
        """Fetch OHLCV data (candles) for symbol `sym` and timeframe `tf`."""
        # Load recent cache (only keep last cache_days of data)
        df_cache = self._load_cache(sym, tf)
        if not df_cache.empty:
            cutoff = df_cache["ts"].max() - pd.Timedelta(days=self.cache_days)
            df_cache = df_cache[df_cache["ts"] >= cutoff]

        miss = limit - len(df_cache)
        rows = []
        end_ms = int(df_cache["ts"].min().timestamp() * 1000) - 1 if not df_cache.empty else None

        try:
            # Fetch missing data in batches (Binance max 1500 per request)
            while miss > 0:
                batch = min(1500, miss)
                params = {"symbol": sym, "interval": tf, "limit": batch}
                if end_ms is not None:
                    params["endTime"] = end_ms
                # Use CCXT to fetch klines (through proxy if configured)
                data = await self.exchange.fapiPublicGetKlines(params)
                if not data:
                    break  # no more data
                rows = data + rows       # prepend new data to maintain chronological order
                end_ms = data[0][0] - 1  # update end to the earliest timestamp fetched - 1ms
                miss -= len(data)
        except Exception as e:
            self.log.error(f"Error fetching klines for {sym}: {e}")
            # (Optional) fallback to direct HTTP if CCXT fails
            try:
                data = await self.http.get(f"{BINANCE}/fapi/v1/klines", params)
                if data:
                    rows = data + rows
            except Exception as e2:
                self.log.error(f"HTTP fallback also failed: {e2}")
                raise
        # Merge fetched data with cache and save
        if rows:
            new_df = pd.DataFrame(rows, columns=[
                "ts", "open", "high", "low", "close", "volume",
                "_1", "_2", "_3", "_4", "_5", "_6"
            ])
            new_df = new_df[["ts", "open", "high", "low", "close", "volume"]]
            new_df["ts"] = pd.to_datetime(new_df["ts"], unit="ms")
            df = pd.concat([df_cache, new_df]).drop_duplicates("ts").sort_values("ts")
            # Trim to cache_days window
            cutoff = df["ts"].max() - pd.Timedelta(days=self.cache_days)
            df = df[df["ts"] >= cutoff]
            self._save_cache(sym, tf, df)
        else:
            df = df_cache  # no new data fetched
        return df.tail(limit)

    async def funding_rate(self, sym):
        """Fetch the latest funding rate for symbol `sym`."""
        base, quote = sym[:-4], sym[-4:]
        symbol = f"{base}/{quote}"
        try:
            res = await self.exchange.fetch_funding_rate(symbol)
            return float(res.get("fundingRate", 0.0))
        except Exception as e:
            self.log.warning(f"CCXT funding_rate failed for {sym}, fallback to HTTP: {e}")
            # Fallback to HTTP endpoint
            data = await self.http.get(f"{BINANCE}/fapi/v1/fundingRate", {"symbol": sym, "limit": 1})
            if data:
                return float(data[-1].get("fundingRate", 0.0))
            return 0.0

    async def long_short_ratio(self, sym, period="2h"):
        """Fetch global long/short account ratio for symbol `sym`."""
        data = await self.http.get(f"{BINANCE}/futures/data/globalLongShortAccountRatio",
                                   {"symbol": sym, "period": period, "limit": 1})
        try:
            return float(data[-1]["longShortRatio"])
        except Exception:
            return 1.0

    async def close(self):
        """Close the CCXT client session."""
        if self.exchange:
            await self.exchange.close()
