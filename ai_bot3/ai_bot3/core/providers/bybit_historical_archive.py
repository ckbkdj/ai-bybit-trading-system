from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import os
import urllib.request
import zipfile
from collections import deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Deque, Iterable, Mapping, Sequence
from urllib.parse import urlparse

from core.providers.bybit_public_pit import BybitPublicPITStore


ORDERBOOK_HOST = "quote-saver.bycsi.com"
TRADE_HOST = "public.bybit.com"
ARCHIVE_MARKET = "linear"
ORDERBOOK_DEPTH = 200


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("archive timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _event_time_ms(payload: Mapping[str, object]) -> datetime:
    raw = payload.get("cts") or payload.get("ts")
    if raw is None:
        raise ValueError("orderbook archive event has no exchange timestamp")
    return datetime.fromtimestamp(int(raw) / 1000.0, timezone.utc)


def orderbook_archive_url(symbol: str, trading_date: date) -> str:
    normalized = symbol.strip().upper()
    stamp = trading_date.isoformat()
    return (
        f"https://{ORDERBOOK_HOST}/orderbook/{ARCHIVE_MARKET}/{normalized}/"
        f"{stamp}_{normalized}_ob{ORDERBOOK_DEPTH}.data.zip"
    )


def trade_archive_url(symbol: str, trading_date: date) -> str:
    normalized = symbol.strip().upper()
    stamp = trading_date.isoformat()
    return f"https://{TRADE_HOST}/trading/{normalized}/{normalized}{stamp}.csv.gz"


@dataclass(frozen=True)
class ArchiveReplayEvidence:
    archive_id: str
    data_kind: str
    market: str
    symbol: str
    trading_date: str
    source_url: str
    fetched_at: str
    content_length: int
    content_sha256: str
    member_name: str | None
    member_size: int | None
    first_event_time: str
    last_event_time: str
    rows_read: int
    feature_observation_count: int
    status: str = "completed"
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_official_archive(
    url: str,
    target: Path,
    *,
    timeout_sec: float = 300.0,
    maximum_bytes: int = 2_000_000_000,
) -> tuple[int, str, datetime]:
    """Download one allow-listed public archive with an atomic file and SHA-256."""

    parsed = urlparse(url)
    allowed_hosts = {ORDERBOOK_HOST, TRADE_HOST}
    if parsed.scheme != "https" or parsed.hostname not in allowed_hosts:
        raise ValueError("archive URL is outside the official Bybit allow-list")
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + f".partial.{os.getpid()}")
    request = urllib.request.Request(url, headers={"User-Agent": "ai-bot3-pit-archive/1"})
    digest = hashlib.sha256()
    written = 0
    fetched_at = datetime.now(timezone.utc)
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > maximum_bytes:
                raise ValueError("archive exceeds configured maximum size")
            with temporary.open("wb") as output:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > maximum_bytes:
                        raise ValueError("archive exceeded configured maximum size")
                    output.write(chunk)
                    digest.update(chunk)
                output.flush()
                os.fsync(output.fileno())
        if declared and written != int(declared):
            raise IOError("archive byte count differs from Content-Length")
        temporary.replace(target)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise
    return written, digest.hexdigest(), fetched_at


def _event_in_archive_day(
    event_time: datetime,
    trading_date: date,
    *,
    maximum_trailing_seconds: float = 0.0,
) -> bool:
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    if event_time < start or event_time > end + timedelta(seconds=maximum_trailing_seconds):
        raise ValueError("archive contains an event outside its UTC trading date")
    return event_time < end


def _archive_id(
    data_kind: str,
    symbol: str,
    trading_date: date,
    source_url: str,
) -> str:
    token = hashlib.sha256(
        f"{data_kind}|{ARCHIVE_MARKET}|{symbol}|{trading_date.isoformat()}|{source_url}".encode()
    ).hexdigest()[:48]
    return f"ba_{token}"


def _record_evidence(store: BybitPublicPITStore, evidence: ArchiveReplayEvidence) -> None:
    store.flush()
    with store.connect() as connection:
        connection.execute(
            """INSERT INTO bybit_historical_archive_files(
                   archive_id,data_kind,market,symbol,trading_date,source_url,
                   fetched_at,content_length,content_sha256,member_name,member_size,
                   first_event_time,last_event_time,rows_read,
                   feature_observation_count,status,error
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(data_kind,market,symbol,trading_date) DO UPDATE SET
                   archive_id=excluded.archive_id,
                   source_url=excluded.source_url,
                   fetched_at=excluded.fetched_at,
                   content_length=excluded.content_length,
                   content_sha256=excluded.content_sha256,
                   member_name=excluded.member_name,
                   member_size=excluded.member_size,
                   first_event_time=excluded.first_event_time,
                   last_event_time=excluded.last_event_time,
                   rows_read=excluded.rows_read,
                   feature_observation_count=excluded.feature_observation_count,
                   status=excluded.status,
                   error=excluded.error
               WHERE bybit_historical_archive_files.status <> 'completed'""",
            (
                evidence.archive_id,
                evidence.data_kind,
                evidence.market,
                evidence.symbol,
                evidence.trading_date,
                evidence.source_url,
                evidence.fetched_at,
                evidence.content_length,
                evidence.content_sha256,
                evidence.member_name,
                evidence.member_size,
                evidence.first_event_time,
                evidence.last_event_time,
                evidence.rows_read,
                evidence.feature_observation_count,
                evidence.status,
                evidence.error,
            ),
        )
        connection.commit()


def archive_already_completed(
    store: BybitPublicPITStore,
    *,
    data_kind: str,
    symbol: str,
    trading_date: date,
) -> bool:
    store.flush()
    with store.connect() as connection:
        row = connection.execute(
            """SELECT status FROM bybit_historical_archive_files
                 WHERE data_kind=? AND market=? AND symbol=? AND trading_date=?""",
            (data_kind, ARCHIVE_MARKET, symbol.strip().upper(), trading_date.isoformat()),
        ).fetchone()
    return bool(row and row["status"] == "completed")


def record_archive_failure(
    store: BybitPublicPITStore,
    *,
    data_kind: str,
    symbol: str,
    trading_date: date,
    source_url: str,
    fetched_at: datetime,
    error: str,
    content_length: int = 0,
    content_sha256: str = "",
) -> ArchiveReplayEvidence:
    normalized = symbol.strip().upper()
    evidence = ArchiveReplayEvidence(
        archive_id=_archive_id(data_kind, normalized, trading_date, source_url),
        data_kind=data_kind,
        market=ARCHIVE_MARKET,
        symbol=normalized,
        trading_date=trading_date.isoformat(),
        source_url=source_url,
        fetched_at=_iso(fetched_at),
        content_length=max(0, int(content_length)),
        content_sha256=str(content_sha256),
        member_name=None,
        member_size=None,
        first_event_time="",
        last_event_time="",
        rows_read=0,
        feature_observation_count=0,
        status="failed",
        error=str(error)[:2_000],
    )
    _record_evidence(store, evidence)
    return evidence


@dataclass
class _ReplayBook:
    bids: dict[float, float]
    asks: dict[float, float]
    best_bid: float | None = None
    best_bid_size: float | None = None
    best_ask: float | None = None
    best_ask_size: float | None = None
    cross_sequence: int | None = None
    valid: bool = False
    last_emitted_imbalance: float | None = None
    last_emit_at: datetime | None = None


def _apply_levels(target: dict[float, float], levels: Sequence[Sequence[object]]) -> None:
    for level in levels:
        if len(level) < 2:
            continue
        price = float(level[0])
        size = float(level[1])
        if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size < 0:
            raise ValueError("invalid historical orderbook level")
        if size == 0:
            target.pop(price, None)
        else:
            target[price] = size


def _best(book: _ReplayBook) -> tuple[float, float, float, float]:
    if not book.bids or not book.asks:
        raise ValueError("historical orderbook is empty")
    best_bid = max(book.bids)
    best_ask = min(book.asks)
    if best_ask <= best_bid:
        raise ValueError("historical orderbook is crossed")
    return best_bid, book.bids[best_bid], best_ask, book.asks[best_ask]


def _archive_feature(
    *,
    event_id: str,
    symbol: str,
    name: str,
    value: float,
    unit: str,
    event_time: datetime,
    available_at: datetime,
    ingested_at: datetime,
    source: str,
    quality: float = 0.96,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "symbol": symbol,
        "name": name,
        "value": value,
        "unit": unit,
        "event_time": event_time,
        "available_at": available_at,
        "ingested_at": ingested_at,
        "source": source,
        "quality": quality,
    }


def replay_orderbook_archive(
    store: BybitPublicPITStore,
    path: Path,
    *,
    symbol: str,
    trading_date: date,
    source_url: str,
    fetched_at: datetime,
    content_sha256: str | None = None,
    feature_emit_interval_sec: float = 15.0,
    assumed_feed_latency_ms: int = 1_000,
    execution_probe_notional_usdt: float = 1_000.0,
) -> ArchiveReplayEvidence:
    """Replay an official daily L2 archive into sampled, strict-PIT features."""

    if feature_emit_interval_sec <= 0 or assumed_feed_latency_ms < 0:
        raise ValueError("invalid archive replay cadence or latency")
    normalized = symbol.strip().upper()
    if source_url != orderbook_archive_url(normalized, trading_date):
        raise ValueError("orderbook archive source URL does not match its contract")
    ingested_at = _utc(fetched_at)
    actual_sha256 = _sha256_file(path)
    if content_sha256 is not None and content_sha256 != actual_sha256:
        raise ValueError("orderbook archive SHA-256 changed before replay")
    content_sha256 = actual_sha256
    content_length = Path(path).stat().st_size
    archive_id = _archive_id("orderbook", normalized, trading_date, source_url)
    book = _ReplayBook({}, {})
    ofi_window: Deque[tuple[datetime, float]] = deque()
    rows_read = 0
    pending_features: list[dict[str, object]] = []
    first_event: datetime | None = None
    last_event: datetime | None = None
    with zipfile.ZipFile(path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if len(members) != 1:
            raise ValueError("orderbook archive must contain exactly one data member")
        member = members[0]
        if member.file_size > 5_000_000_000:
            raise ValueError("orderbook archive member exceeds the replay size limit")
        expected_member = (
            f"{trading_date.isoformat()}_{normalized}_ob{ORDERBOOK_DEPTH}.data"
        )
        if member.filename != expected_member:
            raise ValueError("orderbook archive member name does not match its contract")
        with archive.open(member) as stream:
            for raw_line in stream:
                if not raw_line.strip():
                    continue
                if len(raw_line) > 10_000_000:
                    raise ValueError("orderbook archive line exceeds the replay size limit")
                message = json.loads(raw_line)
                topic = str(message.get("topic") or "")
                data = dict(message.get("data") or {})
                if topic != f"orderbook.{ORDERBOOK_DEPTH}.{normalized}":
                    raise ValueError("orderbook archive topic does not match requested symbol")
                if str(data.get("s") or "").upper() != normalized:
                    raise ValueError("orderbook archive payload symbol mismatch")
                event_time = _event_time_ms(message)
                in_trading_day = _event_in_archive_day(
                    event_time,
                    trading_date,
                    maximum_trailing_seconds=10.0,
                )
                if last_event is not None and event_time < last_event:
                    raise ValueError("orderbook archive event time regressed")
                available_at = event_time + timedelta(milliseconds=assumed_feed_latency_ms)
                if available_at > ingested_at:
                    raise ValueError("archive was fetched before its simulated availability")
                event_type = str(message.get("type") or "delta").lower()
                cross_sequence = int(data.get("seq") or 0)
                rows_read += 1
                first_event = first_event or event_time
                last_event = event_time
                # Official daily files contain the next day's opening
                # snapshot and a few preceding deltas. Validate that narrow
                # overlap, but assign it only to the next UTC archive.
                if not in_trading_day:
                    continue
                is_snapshot = event_type == "snapshot"
                if (
                    not is_snapshot
                    and book.cross_sequence is not None
                    and cross_sequence < book.cross_sequence
                ):
                    raise ValueError("orderbook archive cross-sequence regressed")
                if is_snapshot:
                    book.bids.clear()
                    book.asks.clear()
                    book.valid = True
                    book.last_emitted_imbalance = None
                    ofi_window.clear()
                elif not book.valid:
                    raise ValueError("orderbook archive did not start with a snapshot")

                old = (
                    book.best_bid,
                    book.best_bid_size,
                    book.best_ask,
                    book.best_ask_size,
                )
                _apply_levels(book.bids, data.get("b") or [])
                _apply_levels(book.asks, data.get("a") or [])
                best_bid, best_bid_size, best_ask, best_ask_size = _best(book)
                if all(value is not None for value in old):
                    old_bid, old_bid_size, old_ask, old_ask_size = old
                    contribution = (
                        (best_bid_size if best_bid >= float(old_bid) else 0.0)
                        - (float(old_bid_size) if best_bid <= float(old_bid) else 0.0)
                        - (best_ask_size if best_ask <= float(old_ask) else 0.0)
                        + (float(old_ask_size) if best_ask >= float(old_ask) else 0.0)
                    )
                    ofi_window.append((available_at, float(contribution)))
                book.best_bid = best_bid
                book.best_bid_size = best_bid_size
                book.best_ask = best_ask
                book.best_ask_size = best_ask_size
                book.cross_sequence = cross_sequence
                cutoff = available_at - timedelta(minutes=1)
                while ofi_window and ofi_window[0][0] < cutoff:
                    ofi_window.popleft()

                should_emit = is_snapshot or book.last_emit_at is None or (
                    available_at - book.last_emit_at
                ).total_seconds() >= feature_emit_interval_sec
                if should_emit:
                    bids = sorted(book.bids.items(), reverse=True)[:5]
                    asks = sorted(book.asks.items())[:5]
                    bid_depth = sum(price * size for price, size in bids)
                    ask_depth = sum(price * size for price, size in asks)
                    total_depth = bid_depth + ask_depth
                    imbalance = (
                        (bid_depth - ask_depth) / total_depth if total_depth else 0.0
                    )
                    delta = imbalance - (
                        book.last_emitted_imbalance
                        if book.last_emitted_imbalance is not None
                        else imbalance
                    )
                    midpoint = (best_bid + best_ask) / 2.0
                    microprice = (
                        best_ask * best_bid_size + best_bid * best_ask_size
                    ) / max(best_bid_size + best_ask_size, 1e-12)

                    def sweep(levels: Sequence[tuple[float, float]]) -> tuple[float, float]:
                        remaining = execution_probe_notional_usdt
                        filled = 0.0
                        quantity = 0.0
                        for price, size in levels:
                            take = min(remaining, price * size)
                            filled += take
                            quantity += take / price
                            remaining -= take
                            if remaining <= 1e-9:
                                break
                        vwap = filled / quantity if quantity else midpoint
                        return filled / execution_probe_notional_usdt, vwap

                    ask_fill, ask_vwap = sweep(asks)
                    bid_fill, bid_vwap = sweep(bids)
                    factors = {
                        "orderbook_spread_bps": (
                            (best_ask - best_bid) / midpoint * 10_000.0,
                            "bps",
                        ),
                        "bybit_orderbook_delta_l5": (delta, "ratio"),
                        "ofi_1m": (
                            sum(value for _, value in ofi_window),
                            "base_asset",
                        ),
                        "orderbook_imbalance_l5": (imbalance, "ratio"),
                        "orderbook_depth_usdt_l5": (total_depth, "usd"),
                        "microprice_deviation_bps": (
                            (microprice - midpoint) / midpoint * 10_000.0,
                            "bps",
                        ),
                        "fill_probability": (min(ask_fill, bid_fill), "probability"),
                        "expected_slippage_bps": (
                            max(
                                0.0,
                                (
                                    (ask_vwap - midpoint) / midpoint
                                    + (midpoint - bid_vwap) / midpoint
                                )
                                * 5_000.0,
                            ),
                            "bps",
                        ),
                    }
                    event_id = (
                        f"archive-book:{content_sha256[:16]}:{cross_sequence}:"
                        f"{int(event_time.timestamp() * 1000)}"
                    )
                    for name, (value, unit) in factors.items():
                        pending_features.append(
                            _archive_feature(
                                event_id=event_id,
                                symbol=normalized,
                                name=name,
                                value=value,
                                unit=unit,
                                event_time=event_time,
                                available_at=available_at,
                                ingested_at=ingested_at,
                                source="bybit.public.orderbook",
                            )
                        )
                    book.last_emit_at = available_at
                    book.last_emitted_imbalance = imbalance
    if rows_read == 0 or first_event is None or last_event is None:
        raise ValueError("orderbook archive contained no events")
    archive_record = {
        "archive_id": archive_id,
        "data_kind": "orderbook",
        "market": ARCHIVE_MARKET,
        "symbol": normalized,
        "trading_date": trading_date.isoformat(),
        "source_url": source_url,
        "fetched_at": _iso(ingested_at),
        "content_length": content_length,
        "content_sha256": content_sha256,
        "member_name": member.filename,
        "member_size": member.file_size,
        "first_event_time": _iso(first_event),
        "last_event_time": _iso(last_event),
        "rows_read": rows_read,
    }
    feature_count = store.append_feature_batch(
        pending_features,
        archive_record=archive_record,
    )
    evidence = ArchiveReplayEvidence(
        archive_id=archive_id,
        data_kind="orderbook",
        market=ARCHIVE_MARKET,
        symbol=normalized,
        trading_date=trading_date.isoformat(),
        source_url=source_url,
        fetched_at=_iso(ingested_at),
        content_length=content_length,
        content_sha256=content_sha256,
        member_name=member.filename,
        member_size=member.file_size,
        first_event_time=_iso(first_event),
        last_event_time=_iso(last_event),
        rows_read=rows_read,
        feature_observation_count=feature_count,
    )
    return evidence


def _trade_rows(path: Path) -> Iterable[dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"timestamp", "symbol", "side", "size", "price", "trdMatchID"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError("trade archive schema is missing required columns")
        yield from reader


def replay_trade_archive(
    store: BybitPublicPITStore,
    path: Path,
    *,
    symbol: str,
    trading_date: date,
    source_url: str,
    fetched_at: datetime,
    content_sha256: str | None = None,
    feature_emit_interval_sec: float = 15.0,
    assumed_feed_latency_ms: int = 1_000,
) -> ArchiveReplayEvidence:
    """Replay official public trades into rolling one-minute CVD features."""

    if feature_emit_interval_sec <= 0 or assumed_feed_latency_ms < 0:
        raise ValueError("invalid archive replay cadence or latency")
    normalized = symbol.strip().upper()
    if source_url != trade_archive_url(normalized, trading_date):
        raise ValueError("trade archive source URL does not match its contract")
    ingested_at = _utc(fetched_at)
    actual_sha256 = _sha256_file(path)
    if content_sha256 is not None and content_sha256 != actual_sha256:
        raise ValueError("trade archive SHA-256 changed before replay")
    content_sha256 = actual_sha256
    content_length = Path(path).stat().st_size
    archive_id = _archive_id("trades", normalized, trading_date, source_url)
    window: Deque[tuple[datetime, float, str]] = deque()
    recent_ids: set[str] = set()
    recent_id_times: Deque[tuple[datetime, str]] = deque()
    buy_total = 0.0
    sell_total = 0.0
    rows_read = 0
    pending_features: list[dict[str, object]] = []
    first_event: datetime | None = None
    last_event: datetime | None = None
    last_emit: datetime | None = None
    for row in _trade_rows(path):
        row_symbol = str(row["symbol"]).upper()
        if row_symbol != normalized:
            raise ValueError("trade archive payload symbol mismatch")
        event_time = datetime.fromtimestamp(float(row["timestamp"]), timezone.utc)
        _event_in_archive_day(event_time, trading_date)
        if last_event is not None and event_time < last_event:
            raise ValueError("trade archive event time regressed")
        available_at = event_time + timedelta(milliseconds=assumed_feed_latency_ms)
        if available_at > ingested_at:
            raise ValueError("trade archive was fetched before its simulated availability")
        trade_id = str(row["trdMatchID"])
        duplicate_cutoff = available_at - timedelta(minutes=2)
        while recent_id_times and recent_id_times[0][0] < duplicate_cutoff:
            _, expired = recent_id_times.popleft()
            recent_ids.discard(expired)
        if trade_id in recent_ids:
            continue
        recent_ids.add(trade_id)
        recent_id_times.append((available_at, trade_id))
        size = float(row["size"])
        price = float(row["price"])
        side = str(row["side"]).strip().lower()
        if not math.isfinite(size) or not math.isfinite(price) or size <= 0 or price <= 0:
            raise ValueError("trade archive contains invalid price or size")
        if side not in {"buy", "sell"}:
            raise ValueError("trade archive contains an invalid aggressor side")
        window.append((available_at, size, side))
        if side == "buy":
            buy_total += size
        else:
            sell_total += size
        cutoff = available_at - timedelta(minutes=1)
        while window and window[0][0] < cutoff:
            _, expired_size, expired_side = window.popleft()
            if expired_side == "buy":
                buy_total -= expired_size
            else:
                sell_total -= expired_size
        should_emit = last_emit is None or (
            available_at - last_emit
        ).total_seconds() >= feature_emit_interval_sec
        if should_emit:
            total = buy_total + sell_total
            factors = {
                "public_trade_imbalance_1m": (
                    (buy_total - sell_total) / total if total else 0.0,
                    "ratio",
                ),
                "aggressive_cvd_1m": (buy_total - sell_total, "base_asset"),
            }
            event_id = f"archive-trade:{content_sha256[:16]}:{trade_id}"
            for name, (value, unit) in factors.items():
                pending_features.append(
                    _archive_feature(
                        event_id=event_id,
                        symbol=normalized,
                        name=name,
                        value=value,
                        unit=unit,
                        event_time=event_time,
                        available_at=available_at,
                        ingested_at=ingested_at,
                        source="bybit.public.trades",
                    )
                )
            last_emit = available_at
        rows_read += 1
        first_event = first_event or event_time
        last_event = event_time
    if rows_read == 0 or first_event is None or last_event is None:
        raise ValueError("trade archive contained no events")
    archive_record = {
        "archive_id": archive_id,
        "data_kind": "trades",
        "market": ARCHIVE_MARKET,
        "symbol": normalized,
        "trading_date": trading_date.isoformat(),
        "source_url": source_url,
        "fetched_at": _iso(ingested_at),
        "content_length": content_length,
        "content_sha256": content_sha256,
        "member_name": None,
        "member_size": None,
        "first_event_time": _iso(first_event),
        "last_event_time": _iso(last_event),
        "rows_read": rows_read,
    }
    feature_count = store.append_feature_batch(
        pending_features,
        archive_record=archive_record,
    )
    evidence = ArchiveReplayEvidence(
        archive_id=archive_id,
        data_kind="trades",
        market=ARCHIVE_MARKET,
        symbol=normalized,
        trading_date=trading_date.isoformat(),
        source_url=source_url,
        fetched_at=_iso(ingested_at),
        content_length=content_length,
        content_sha256=content_sha256,
        member_name=None,
        member_size=None,
        first_event_time=_iso(first_event),
        last_event_time=_iso(last_event),
        rows_read=rows_read,
        feature_observation_count=feature_count,
    )
    return evidence


__all__: Sequence[str] = (
    "ArchiveReplayEvidence",
    "archive_already_completed",
    "download_official_archive",
    "orderbook_archive_url",
    "record_archive_failure",
    "replay_orderbook_archive",
    "replay_trade_archive",
    "trade_archive_url",
)
