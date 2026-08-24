import asyncio
import time

from core.data_fetch import DataFetcher, _closed_kline_rows


def _row(open_ms: int, close_ms: int):
    return [open_ms, "1", "2", "0.5", "1.5", "10", close_ms, "0", 1, "0", "0", "0"]


def test_live_fetch_excludes_unfinished_exchange_candle():
    now_ms = 1_000_000
    closed = _row(700_000, 879_999)
    still_open = _row(880_000, 1_059_999)
    assert _closed_kline_rows([closed, still_open], "3m", now_ms=now_ms) == [closed]


def test_profitability_runtime_fetches_fresh_completed_bybit_last_trade_klines():
    interval_ms = 180_000
    current_open = int(time.time() * 1_000) // interval_ms * interval_ms
    rows = []
    for index in range(51):
        open_ms = current_open - index * interval_ms
        price = 100 + index
        rows.append(
            [
                str(open_ms),
                str(price),
                str(price + 2),
                str(price - 1),
                str(price + 1),
                "10",
                "1000",
            ]
        )

    class HTTP:
        async def get(self, url, params=None, **kwargs):
            assert url.endswith("/v5/market/kline")
            assert params == {
                "category": "linear",
                "symbol": "BTCUSDT",
                "interval": "3",
                "limit": 60,
            }
            return {
                "retCode": 0,
                "result": {
                    "category": "linear",
                    "symbol": "BTCUSDT",
                    "list": rows,
                },
            }

    fetcher = object.__new__(DataFetcher)
    fetcher.http = HTTP()
    frame = asyncio.run(fetcher.get_bybit_ohlcv("BTCUSDT", "3m", 60))

    assert len(frame) == 50
    assert frame["ts"].is_monotonic_increasing
    assert frame.attrs["data_source"] == "bybit_linear_last_trade_kline"
    assert frame["close_at"].iloc[-1].timestamp() * 1_000 == current_open
