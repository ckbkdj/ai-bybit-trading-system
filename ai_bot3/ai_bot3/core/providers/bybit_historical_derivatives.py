from __future__ import annotations

import hashlib
import json
import math
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Mapping, Sequence
from urllib.parse import urlencode, urlparse

from core.providers.bybit_public_pit import BybitPublicPITStore


API_HOST = "api.bybit.com"
API_BASE = f"https://{API_HOST}"
MARKET = "linear"
ALLOWED_PATHS = {
    "/v5/market/funding/history",
    "/v5/market/open-interest",
    "/v5/market/mark-price-kline",
    "/v5/market/index-price-kline",
}
DATA_KINDS = ("funding", "open_interest", "basis")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("historical API timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _ms(value: datetime) -> int:
    return int(_utc(value).timestamp() * 1000)


def _from_ms(value: object) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000.0, timezone.utc)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _finite(value: object, *, nonnegative: bool = False) -> float:
    number = float(value)
    if not math.isfinite(number) or (nonnegative and number < 0):
        raise ValueError("Bybit historical API returned an invalid numeric value")
    return number


def _day_bounds(trading_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _require_complete_grid(
    values: Mapping[int, object],
    *,
    start: datetime,
    end: datetime,
    interval_sec: int,
    label: str,
) -> None:
    if interval_sec <= 0:
        raise ValueError("historical grid interval must be positive")
    interval_ms = interval_sec * 1000
    expected = set(range(_ms(start), _ms(end), interval_ms))
    actual = set(values)
    if actual != expected:
        missing = len(expected.difference(actual))
        unexpected = len(actual.difference(expected))
        raise ValueError(
            f"{label} history grid is incomplete: "
            f"missing={missing}, unexpected={unexpected}"
        )


def _url(path: str, parameters: Mapping[str, object]) -> str:
    if path not in ALLOWED_PATHS:
        raise ValueError("Bybit historical API path is not allow-listed")
    return f"{API_BASE}{path}?{urlencode(sorted(parameters.items()))}"


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != API_HOST
        or parsed.path not in ALLOWED_PATHS
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("historical API URL is outside the official Bybit allow-list")


@dataclass(frozen=True)
class HTTPPayload:
    body: bytes
    requested_at: datetime
    received_at: datetime
    http_status: int


@dataclass(frozen=True)
class APIResponseEvidence:
    response_id: str
    batch_id: str
    request_url: str
    requested_at: str
    received_at: str
    http_status: int
    content_length: int
    content_sha256: str
    content_blob: bytes
    rows_read: int
    ret_code: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class HistoricalAPIBatchEvidence:
    batch_id: str
    data_kind: str
    market: str
    symbol: str
    trading_date: str
    endpoint_group: str
    requested_at: str
    completed_at: str
    first_event_time: str | None
    last_event_time: str | None
    response_count: int
    rows_read: int
    feature_observation_count: int
    request_manifest_sha256: str
    status: str = "completed"
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


Requester = Callable[[str, float], HTTPPayload]


def _default_request(url: str, timeout_sec: float) -> HTTPPayload:
    _validate_url(url)
    requested_at = datetime.now(timezone.utc)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ai-bot3-pit-derivatives/1"},
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        body = response.read(20_000_001)
        status = int(getattr(response, "status", 200))
    received_at = datetime.now(timezone.utc)
    if len(body) > 20_000_000:
        raise ValueError("Bybit historical API response exceeds 20 MB")
    return HTTPPayload(body, requested_at, received_at, status)


def _batch_id(data_kind: str, symbol: str, trading_date: date) -> str:
    token = hashlib.sha256(
        f"{data_kind}|{MARKET}|{symbol}|{trading_date.isoformat()}".encode()
    ).hexdigest()[:48]
    return f"bh_{token}"


def _fetch(
    url: str,
    *,
    batch_id: str,
    requester: Requester,
    timeout_sec: float,
) -> tuple[Mapping[str, object], APIResponseEvidence]:
    _validate_url(url)
    response = requester(url, timeout_sec)
    requested_at = _utc(response.requested_at)
    received_at = _utc(response.received_at)
    if requested_at > received_at:
        raise ValueError("historical API response chronology is invalid")
    if response.http_status != 200:
        raise RuntimeError(f"Bybit historical API HTTP status {response.http_status}")
    if len(response.body) > 20_000_000:
        raise ValueError("Bybit historical API response exceeds 20 MB")
    try:
        payload = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Bybit historical API returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Bybit historical API JSON root is not an object")
    ret_code = int(payload.get("retCode", -1))
    if ret_code != 0:
        raise RuntimeError(
            f"Bybit historical API rejected request: {ret_code} {payload.get('retMsg')}"
        )
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Bybit historical API response has no result object")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise ValueError("Bybit historical API response has no result list")
    content_sha256 = hashlib.sha256(response.body).hexdigest()
    response_id = "br_" + hashlib.sha256(
        f"{batch_id}|{url}|{content_sha256}".encode()
    ).hexdigest()[:48]
    evidence = APIResponseEvidence(
        response_id=response_id,
        batch_id=batch_id,
        request_url=url,
        requested_at=_iso(requested_at),
        received_at=_iso(received_at),
        http_status=response.http_status,
        content_length=len(response.body),
        content_sha256=content_sha256,
        content_blob=response.body,
        rows_read=len(rows),
        ret_code=ret_code,
    )
    return result, evidence


def _manifest_sha(responses: Sequence[APIResponseEvidence]) -> str:
    manifest = [
        {
            "request_url": item.request_url,
            "content_sha256": item.content_sha256,
            "rows_read": item.rows_read,
            "ret_code": item.ret_code,
        }
        for item in sorted(responses, key=lambda item: item.request_url)
    ]
    return hashlib.sha256(_canonical(manifest)).hexdigest()


def historical_api_batch_completed(
    store: BybitPublicPITStore,
    *,
    data_kind: str,
    symbol: str,
    trading_date: date,
) -> bool:
    store.flush()
    with store.connect() as connection:
        row = connection.execute(
            """SELECT status FROM bybit_historical_api_batches
                 WHERE data_kind=? AND market=? AND symbol=? AND trading_date=?""",
            (data_kind, MARKET, symbol.strip().upper(), trading_date.isoformat()),
        ).fetchone()
    return bool(row and str(row["status"]) == "completed")


def record_historical_api_failure(
    store: BybitPublicPITStore,
    *,
    data_kind: str,
    symbol: str,
    trading_date: date,
    error: str,
) -> None:
    if data_kind not in DATA_KINDS:
        raise ValueError("unsupported Bybit historical derivative kind")
    now = datetime.now(timezone.utc)
    batch_id = _batch_id(data_kind, symbol.strip().upper(), trading_date)
    failure_sha = hashlib.sha256(
        _canonical(
            {
                "data_kind": data_kind,
                "symbol": symbol.strip().upper(),
                "trading_date": trading_date.isoformat(),
                "error": str(error),
            }
        )
    ).hexdigest()
    store.flush()
    with store.connect() as connection:
        connection.execute(
            """INSERT INTO bybit_historical_api_batches(
                   batch_id,data_kind,market,symbol,trading_date,endpoint_group,
                   requested_at,completed_at,first_event_time,last_event_time,
                   response_count,rows_read,feature_observation_count,
                   request_manifest_sha256,status,error
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(data_kind,market,symbol,trading_date) DO UPDATE SET
                   batch_id=excluded.batch_id,
                   requested_at=excluded.requested_at,
                   completed_at=excluded.completed_at,
                   response_count=0,rows_read=0,feature_observation_count=0,
                   request_manifest_sha256=excluded.request_manifest_sha256,
                   status='failed',error=excluded.error
               WHERE bybit_historical_api_batches.status <> 'completed'""",
            (
                batch_id,
                data_kind,
                MARKET,
                symbol.strip().upper(),
                trading_date.isoformat(),
                data_kind,
                _iso(now),
                _iso(now),
                None,
                None,
                0,
                0,
                0,
                failure_sha,
                "failed",
                str(error)[:2000],
            ),
        )
        connection.commit()


def _batch_evidence(
    *,
    batch_id: str,
    data_kind: str,
    symbol: str,
    trading_date: date,
    endpoint_group: str,
    responses: Sequence[APIResponseEvidence],
    event_times: Sequence[datetime],
    feature_count: int,
) -> HistoricalAPIBatchEvidence:
    if not responses:
        raise ValueError("a completed historical API batch requires response evidence")
    requested_at = min(item.requested_at for item in responses)
    completed_at = max(item.received_at for item in responses)
    ordered_events = sorted(_utc(value) for value in event_times)
    return HistoricalAPIBatchEvidence(
        batch_id=batch_id,
        data_kind=data_kind,
        market=MARKET,
        symbol=symbol,
        trading_date=trading_date.isoformat(),
        endpoint_group=endpoint_group,
        requested_at=requested_at,
        completed_at=completed_at,
        first_event_time=_iso(ordered_events[0]) if ordered_events else None,
        last_event_time=_iso(ordered_events[-1]) if ordered_events else None,
        response_count=len(responses),
        rows_read=sum(item.rows_read for item in responses),
        feature_observation_count=feature_count,
        request_manifest_sha256=_manifest_sha(responses),
    )


def replay_funding_day(
    store: BybitPublicPITStore,
    *,
    symbol: str,
    trading_date: date,
    requester: Requester = _default_request,
    timeout_sec: float = 30.0,
) -> HistoricalAPIBatchEvidence:
    normalized = symbol.strip().upper()
    start, end = _day_bounds(trading_date)
    batch_id = _batch_id("funding", normalized, trading_date)
    url = _url(
        "/v5/market/funding/history",
        {
            "category": MARKET,
            "symbol": normalized,
            "startTime": _ms(start),
            "endTime": _ms(end) - 1,
            "limit": 200,
        },
    )
    result, response = _fetch(
        url, batch_id=batch_id, requester=requester, timeout_sec=timeout_sec
    )
    observations: list[dict[str, object]] = []
    seen: dict[int, float] = {}
    ingested_at = _utc(datetime.fromisoformat(response.received_at.replace("Z", "+00:00")))
    for raw in result["list"]:  # type: ignore[index]
        if not isinstance(raw, dict) or str(raw.get("symbol")) != normalized:
            raise ValueError("funding history symbol contract failed")
        timestamp_ms = int(raw["fundingRateTimestamp"])
        event_time = _from_ms(timestamp_ms)
        if not start <= event_time < end:
            raise ValueError("funding history row is outside the requested UTC day")
        value = _finite(raw["fundingRate"])
        if abs(value) > 1:
            raise ValueError("funding history rate is outside a defensible range")
        if timestamp_ms in seen and seen[timestamp_ms] != value:
            raise ValueError("funding history contains conflicting duplicate timestamps")
        seen[timestamp_ms] = value
    if not seen:
        raise ValueError("funding history has no settled events for the UTC day")
    ordered_funding_times = [_from_ms(value) for value in sorted(seen)]
    funding_gaps = [ordered_funding_times[0] - start]
    funding_gaps.extend(
        later - earlier
        for earlier, later in zip(
            ordered_funding_times, ordered_funding_times[1:]
        )
    )
    funding_gaps.append(end - ordered_funding_times[-1])
    if max(funding_gaps) > timedelta(hours=8, seconds=1):
        raise ValueError("funding history does not continuously cover the UTC day")
    for timestamp_ms, value in sorted(seen.items()):
        event_time = _from_ms(timestamp_ms)
        available_at = event_time + timedelta(seconds=60)
        if available_at > ingested_at:
            raise ValueError("funding history has not yet become PIT-available")
        observations.append(
            {
                "event_id": f"bybit-funding-history|{normalized}|{timestamp_ms}",
                "symbol": normalized,
                "name": "funding_rate",
                "value": value,
                "unit": "ratio",
                "event_time": event_time,
                "available_at": available_at,
                "ingested_at": ingested_at,
                "source": "bybit.public.funding_history",
                "quality": 1.0,
            }
        )
    evidence = _batch_evidence(
        batch_id=batch_id,
        data_kind="funding",
        symbol=normalized,
        trading_date=trading_date,
        endpoint_group="/v5/market/funding/history",
        responses=[response],
        event_times=[item["event_time"] for item in observations],  # type: ignore[list-item]
        feature_count=len(observations),
    )
    inserted = store.append_feature_batch(
        observations,
        api_batch_record=evidence.to_dict(),
        api_response_records=[response.to_dict()],
    )
    return HistoricalAPIBatchEvidence(
        **{**evidence.to_dict(), "feature_observation_count": inserted}
    )


def replay_open_interest_day(
    store: BybitPublicPITStore,
    *,
    symbol: str,
    trading_date: date,
    requester: Requester = _default_request,
    timeout_sec: float = 30.0,
) -> HistoricalAPIBatchEvidence:
    normalized = symbol.strip().upper()
    start, end = _day_bounds(trading_date)
    midpoint = start + timedelta(hours=12)
    batch_id = _batch_id("open_interest", normalized, trading_date)
    responses: list[APIResponseEvidence] = []
    values: dict[int, float] = {}
    for window_start, window_end in (
        (start - timedelta(hours=1), midpoint),
        (midpoint, end),
    ):
        url = _url(
            "/v5/market/open-interest",
            {
                "category": MARKET,
                "symbol": normalized,
                "intervalTime": "5min",
                "startTime": _ms(window_start),
                "endTime": _ms(window_end) - 1,
                "limit": 200,
            },
        )
        result, response = _fetch(
            url, batch_id=batch_id, requester=requester, timeout_sec=timeout_sec
        )
        if str(result.get("symbol")) != normalized:
            raise ValueError("open-interest response symbol contract failed")
        responses.append(response)
        for raw in result["list"]:  # type: ignore[index]
            if not isinstance(raw, dict):
                raise ValueError("open-interest row is not an object")
            timestamp_ms = int(raw["timestamp"])
            event_time = _from_ms(timestamp_ms)
            if not start - timedelta(hours=1) <= event_time < end:
                raise ValueError("open-interest row is outside the requested window")
            value = _finite(raw["openInterest"], nonnegative=True)
            if timestamp_ms in values and values[timestamp_ms] != value:
                raise ValueError("open-interest history contains a conflicting duplicate")
            values[timestamp_ms] = value
    _require_complete_grid(
        values,
        start=start - timedelta(hours=1),
        end=end,
        interval_sec=300,
        label="open-interest",
    )
    ingested_at = max(
        _utc(datetime.fromisoformat(item.received_at.replace("Z", "+00:00")))
        for item in responses
    )
    observations: list[dict[str, object]] = []
    for timestamp_ms, value in sorted(values.items()):
        event_time = _from_ms(timestamp_ms)
        if not start <= event_time < end:
            continue
        prior = values.get(timestamp_ms - 3_600_000)
        if prior is None or prior <= 0:
            continue
        available_at = event_time + timedelta(seconds=60)
        if available_at > ingested_at:
            raise ValueError("open-interest history has not yet become PIT-available")
        observations.append(
            {
                "event_id": f"bybit-open-interest-history|{normalized}|{timestamp_ms}",
                "symbol": normalized,
                "name": "open_interest_change_1h",
                "value": value / prior - 1.0,
                "unit": "ratio",
                "event_time": event_time,
                "available_at": available_at,
                "ingested_at": ingested_at,
                "source": "bybit.public.open_interest_history",
                "quality": 1.0,
            }
        )
    evidence = _batch_evidence(
        batch_id=batch_id,
        data_kind="open_interest",
        symbol=normalized,
        trading_date=trading_date,
        endpoint_group="/v5/market/open-interest[5min]",
        responses=responses,
        event_times=[item["event_time"] for item in observations],  # type: ignore[list-item]
        feature_count=len(observations),
    )
    inserted = store.append_feature_batch(
        observations,
        api_batch_record=evidence.to_dict(),
        api_response_records=[item.to_dict() for item in responses],
    )
    return HistoricalAPIBatchEvidence(
        **{**evidence.to_dict(), "feature_observation_count": inserted}
    )


def _price_rows(
    result: Mapping[str, object],
    *,
    symbol: str,
    start: datetime,
    end: datetime,
) -> dict[int, float]:
    if str(result.get("symbol")) != symbol:
        raise ValueError("price-kline response symbol contract failed")
    output: dict[int, float] = {}
    for raw in result["list"]:  # type: ignore[index]
        if not isinstance(raw, list) or len(raw) < 5:
            raise ValueError("price-kline row contract failed")
        timestamp_ms = int(raw[0])
        event_time = _from_ms(timestamp_ms)
        if not start <= event_time < end:
            raise ValueError("price-kline row is outside the requested window")
        close_price = _finite(raw[4])
        if close_price <= 0:
            raise ValueError("price-kline close is not positive")
        if timestamp_ms in output and output[timestamp_ms] != close_price:
            raise ValueError("price-kline history contains a conflicting duplicate")
        output[timestamp_ms] = close_price
    return output


def replay_basis_day(
    store: BybitPublicPITStore,
    *,
    symbol: str,
    trading_date: date,
    requester: Requester = _default_request,
    timeout_sec: float = 30.0,
) -> HistoricalAPIBatchEvidence:
    normalized = symbol.strip().upper()
    start, end = _day_bounds(trading_date)
    midpoint = start + timedelta(hours=12)
    batch_id = _batch_id("basis", normalized, trading_date)
    responses: list[APIResponseEvidence] = []
    mark: dict[int, float] = {}
    index: dict[int, float] = {}
    for path, destination in (
        ("/v5/market/mark-price-kline", mark),
        ("/v5/market/index-price-kline", index),
    ):
        for window_start, window_end in ((start, midpoint), (midpoint, end)):
            url = _url(
                path,
                {
                    "category": MARKET,
                    "symbol": normalized,
                    "interval": "1",
                    "start": _ms(window_start),
                    "end": _ms(window_end) - 1,
                    "limit": 1000,
                },
            )
            result, response = _fetch(
                url, batch_id=batch_id, requester=requester, timeout_sec=timeout_sec
            )
            destination.update(
                _price_rows(
                    result,
                    symbol=normalized,
                    start=window_start,
                    end=window_end,
                )
            )
            responses.append(response)
    timestamps = sorted(set(mark).intersection(index))
    _require_complete_grid(
        mark,
        start=start,
        end=end,
        interval_sec=60,
        label="mark-price",
    )
    _require_complete_grid(
        index,
        start=start,
        end=end,
        interval_sec=60,
        label="index-price",
    )
    if set(mark) != set(index):
        raise ValueError("mark and index price histories do not have identical timestamps")
    ingested_at = max(
        _utc(datetime.fromisoformat(item.received_at.replace("Z", "+00:00")))
        for item in responses
    )
    observations: list[dict[str, object]] = []
    for timestamp_ms in timestamps:
        event_time = _from_ms(timestamp_ms)
        available_at = event_time + timedelta(seconds=62)
        if available_at > ingested_at:
            raise ValueError("basis kline has not yet become PIT-available")
        observations.append(
            {
                "event_id": f"bybit-mark-index-basis|{normalized}|{timestamp_ms}",
                "symbol": normalized,
                "name": "perpetual_basis_bps",
                "value": (mark[timestamp_ms] / index[timestamp_ms] - 1.0) * 10_000,
                "unit": "bps",
                "event_time": event_time,
                "available_at": available_at,
                "ingested_at": ingested_at,
                "source": "bybit.public.mark_index_kline",
                "quality": 1.0,
            }
        )
    evidence = _batch_evidence(
        batch_id=batch_id,
        data_kind="basis",
        symbol=normalized,
        trading_date=trading_date,
        endpoint_group="/v5/market/mark-price-kline+/v5/market/index-price-kline[1m]",
        responses=responses,
        event_times=[item["event_time"] for item in observations],  # type: ignore[list-item]
        feature_count=len(observations),
    )
    inserted = store.append_feature_batch(
        observations,
        api_batch_record=evidence.to_dict(),
        api_response_records=[item.to_dict() for item in responses],
    )
    return HistoricalAPIBatchEvidence(
        **{**evidence.to_dict(), "feature_observation_count": inserted}
    )


def replay_derivative_day(
    store: BybitPublicPITStore,
    *,
    data_kind: str,
    symbol: str,
    trading_date: date,
    requester: Requester = _default_request,
    timeout_sec: float = 30.0,
) -> HistoricalAPIBatchEvidence:
    functions = {
        "funding": replay_funding_day,
        "open_interest": replay_open_interest_day,
        "basis": replay_basis_day,
    }
    try:
        function = functions[data_kind]
    except KeyError as exc:
        raise ValueError("unsupported Bybit historical derivative kind") from exc
    return function(
        store,
        symbol=symbol,
        trading_date=trading_date,
        requester=requester,
        timeout_sec=timeout_sec,
    )


__all__ = (
    "API_BASE",
    "API_HOST",
    "APIResponseEvidence",
    "DATA_KINDS",
    "HTTPPayload",
    "HistoricalAPIBatchEvidence",
    "historical_api_batch_completed",
    "record_historical_api_failure",
    "replay_basis_day",
    "replay_derivative_day",
    "replay_funding_day",
    "replay_open_interest_day",
)
