from core.data_fetch import _closed_kline_rows


def _row(open_ms: int, close_ms: int):
    return [open_ms, "1", "2", "0.5", "1.5", "10", close_ms, "0", 1, "0", "0", "0"]


def test_live_fetch_excludes_unfinished_exchange_candle():
    now_ms = 1_000_000
    closed = _row(700_000, 879_999)
    still_open = _row(880_000, 1_059_999)
    assert _closed_kline_rows([closed, still_open], "3m", now_ms=now_ms) == [closed]
