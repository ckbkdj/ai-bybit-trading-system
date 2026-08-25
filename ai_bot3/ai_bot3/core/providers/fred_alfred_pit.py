from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence


API_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
INITIAL_SERIES: tuple[str, ...] = (
    "VIXCLS",
    "DFII10",
    "CPIAUCSL",
    "PAYEMS",
    "UNRATE",
)
REVISION_SERIES: tuple[str, ...] = ("CPIAUCSL", "PAYEMS")
DAILY_INITIAL_SERIES: tuple[str, ...] = ("VIXCLS", "DFII10")
MAX_DAILY_REALTIME_WINDOW_DAYS = 1_460
FEATURE_NAMES: tuple[str, ...] = (
    "vix_level",
    "real_yield_10y",
    "fred_cpi_first_release_yoy_ratio",
    "fred_payrolls_first_release_change_thousands",
    "fred_unemployment_first_release_pct",
    "alfred_cpi_mean_revision_delta",
    "alfred_payrolls_mean_revision_delta",
    "tier_a_event_state",
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _date(value: object) -> date:
    return date.fromisoformat(str(value)[:10])


def _available_at(observation_date: date, vintage_date: date) -> datetime:
    # Some FRED daily series carry a previous business-day value on a later
    # market holiday while retaining the earlier vintage date. Never expose
    # such a row before its nominal observation date.
    conservative_date = max(observation_date, vintage_date)
    return datetime.combine(conservative_date, time(23, 59, 59), timezone.utc)


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _canonical(payload: object) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


@dataclass(frozen=True)
class HTTPPayload:
    body: bytes
    requested_at: datetime
    received_at: datetime
    http_status: int


@dataclass(frozen=True)
class FREDResponseEvidence:
    response_id: str
    series_id: str
    output_type: int
    request_descriptor: str
    requested_at: str
    received_at: str
    http_status: int
    content_length: int
    content_sha256: str
    row_count: int
    raw_response_path: str


Requester = Callable[[str, float], HTTPPayload]


def _default_request(url: str, timeout_sec: float) -> HTTPPayload:
    requested_at = datetime.now(timezone.utc)
    try:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "ai-bot3-fred-alfred-pit/1"},
        )
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read()
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"FRED API returned HTTP {exc.code}") from None
    except Exception as exc:
        # urllib exceptions may embed the query-string API key in their URL.
        raise RuntimeError(f"FRED API request failed: {type(exc).__name__}") from None
    return HTTPPayload(
        body=body,
        requested_at=requested_at,
        received_at=datetime.now(timezone.utc),
        http_status=status,
    )


def _request_url(
    *,
    series_id: str,
    api_key: str,
    output_type: int,
    observation_start: date,
    observation_end: date,
    realtime_start: date | None = None,
    realtime_end: date | None = None,
) -> tuple[str, str]:
    if not re.fullmatch(r"[a-z0-9]{32}", api_key):
        raise ValueError("FRED API key must be a 32-character lowercase token")
    if output_type not in {3, 4}:
        raise ValueError("only strict vintage output types 3 and 4 are supported")
    parameters = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "output_type": output_type,
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "realtime_start": (realtime_start or observation_start).isoformat(),
        "realtime_end": (realtime_end or observation_end).isoformat(),
        "limit": 100_000,
    }
    descriptor = {
        key: value for key, value in parameters.items() if key != "api_key"
    }
    return (
        API_ENDPOINT + "?" + urllib.parse.urlencode(parameters),
        json.dumps(descriptor, sort_keys=True, separators=(",", ":")),
    )


def _realtime_windows(
    start: date,
    end: date,
    *,
    max_days: int | None,
) -> list[tuple[date, date]]:
    if max_days is None:
        return [(start, end)]
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=max_days - 1))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def _float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_initial(series_id: str, payload: Mapping[str, object]) -> list[dict[str, object]]:
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise ValueError("FRED initial-release response has no observations")
    rows: list[dict[str, object]] = []
    for item in observations:
        if not isinstance(item, Mapping):
            continue
        value = _float(item.get("value"))
        if value is None:
            continue
        rows.append(
            {
                "series_id": series_id,
                "observation_date": _date(item.get("date")),
                "vintage_date": _date(item.get("realtime_start")),
                "value": value,
                "version_kind": "initial_release",
            }
        )
    return rows


def _parse_revisions(series_id: str, payload: Mapping[str, object]) -> list[dict[str, object]]:
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise ValueError("ALFRED revision response has no observations")
    pattern = re.compile(rf"^{re.escape(series_id)}_(\d{{8}})$")
    rows: list[dict[str, object]] = []
    for item in observations:
        if not isinstance(item, Mapping):
            continue
        observation_date = _date(item.get("date"))
        versions: list[tuple[date, float]] = []
        for key, raw_value in item.items():
            match = pattern.fullmatch(str(key))
            value = _float(raw_value)
            if match and value is not None:
                versions.append((datetime.strptime(match.group(1), "%Y%m%d").date(), value))
        for position, (vintage_date, value) in enumerate(sorted(versions), start=1):
            rows.append(
                {
                    "series_id": series_id,
                    "observation_date": observation_date,
                    "vintage_date": vintage_date,
                    "value": value,
                    "version_kind": "initial_release" if position == 1 else "revision",
                }
            )
    return rows


def _feature(
    *,
    name: str,
    value: float,
    unit: str,
    series_id: str,
    observation_date: date,
    vintage_date: date,
    ingested_at: datetime,
    source: str,
) -> dict[str, object]:
    available_at = _available_at(observation_date, vintage_date)
    observation_id = hashlib.sha256(
        f"{name}|{series_id}|{observation_date}|{vintage_date}".encode()
    ).hexdigest()[:32]
    return {
        "observation_id": observation_id,
        "name": name,
        "value": float(value),
        "unit": unit,
        "event_time": datetime.combine(observation_date, time(), timezone.utc),
        "available_at": available_at,
        "ingested_at": ingested_at,
        "source": source,
        "series_id": series_id,
        "observation_date": observation_date.isoformat(),
        "vintage_date": vintage_date.isoformat(),
    }


def _month_offset(value: date, months: int) -> tuple[int, int]:
    total = value.year * 12 + value.month - 1 + months
    return total // 12, total % 12 + 1


def _derive_features(
    versions: Sequence[Mapping[str, object]],
    *,
    ingested_at: datetime,
) -> list[dict[str, object]]:
    initial_by_series: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    by_observation: dict[tuple[str, date], list[Mapping[str, object]]] = defaultdict(list)
    for row in versions:
        series_id = str(row["series_id"])
        observation_date = row["observation_date"]
        assert isinstance(observation_date, date)
        by_observation[(series_id, observation_date)].append(row)
        if row["version_kind"] == "initial_release":
            initial_by_series[series_id].append(row)

    features: list[dict[str, object]] = []
    direct = {
        "VIXCLS": ("vix_level", "index_points"),
        "DFII10": ("real_yield_10y", "percent"),
        "UNRATE": ("fred_unemployment_first_release_pct", "percent"),
    }
    for series_id, (name, unit) in direct.items():
        for row in sorted(
            initial_by_series.get(series_id, ()),
            key=lambda item: (item["vintage_date"], item["observation_date"]),
        ):
            features.append(
                _feature(
                    name=name,
                    value=float(row["value"]),
                    unit=unit,
                    series_id=series_id,
                    observation_date=row["observation_date"],  # type: ignore[arg-type]
                    vintage_date=row["vintage_date"],  # type: ignore[arg-type]
                    ingested_at=ingested_at,
                    source="fred.alfred.initial_release",
                )
            )

    cpi_initial = {
        (row["observation_date"].year, row["observation_date"].month): row
        for row in initial_by_series.get("CPIAUCSL", ())
    }
    for row in sorted(cpi_initial.values(), key=lambda item: item["observation_date"]):
        observation_date = row["observation_date"]
        prior = cpi_initial.get(_month_offset(observation_date, -12))
        if prior is None or float(prior["value"]) <= 0:
            continue
        features.append(
            _feature(
                name="fred_cpi_first_release_yoy_ratio",
                value=float(row["value"]) / float(prior["value"]) - 1.0,
                unit="ratio",
                series_id="CPIAUCSL",
                observation_date=observation_date,  # type: ignore[arg-type]
                vintage_date=row["vintage_date"],  # type: ignore[arg-type]
                ingested_at=ingested_at,
                source="fred.alfred.initial_release",
            )
        )

    payrolls = sorted(
        initial_by_series.get("PAYEMS", ()), key=lambda item: item["observation_date"]
    )
    for previous, current in zip(payrolls, payrolls[1:]):
        features.append(
            _feature(
                name="fred_payrolls_first_release_change_thousands",
                value=float(current["value"]) - float(previous["value"]),
                unit="thousands_of_persons",
                series_id="PAYEMS",
                observation_date=current["observation_date"],  # type: ignore[arg-type]
                vintage_date=current["vintage_date"],  # type: ignore[arg-type]
                ingested_at=ingested_at,
                source="fred.alfred.initial_release",
            )
        )

    revision_names = {
        "CPIAUCSL": "alfred_cpi_mean_revision_delta",
        "PAYEMS": "alfred_payrolls_mean_revision_delta",
    }
    revisions_by_release: dict[tuple[str, date], list[float]] = defaultdict(list)
    for (series_id, _), rows in by_observation.items():
        ordered = sorted(rows, key=lambda item: item["vintage_date"])
        for previous, current in zip(ordered, ordered[1:]):
            revisions_by_release[(series_id, current["vintage_date"])].append(  # type: ignore[index]
                float(current["value"]) - float(previous["value"])
            )
    for (series_id, vintage_date), values in sorted(revisions_by_release.items()):
        if series_id not in revision_names:
            continue
        features.append(
            _feature(
                name=revision_names[series_id],
                value=sum(values) / len(values),
                unit="source_units",
                series_id=series_id,
                observation_date=vintage_date,
                vintage_date=vintage_date,
                ingested_at=ingested_at,
                source="fred.alfred.revision_history",
            )
        )

    event_dates = {
        row["vintage_date"]
        for series_id in ("CPIAUCSL", "PAYEMS")
        for row in initial_by_series.get(series_id, ())
    }
    for vintage_date in sorted(event_dates):
        features.append(
            _feature(
                name="tier_a_event_state",
                value=1.0,
                unit="binary_24h_post_release_window",
                series_id="TIER_A",
                observation_date=vintage_date,  # type: ignore[arg-type]
                vintage_date=vintage_date,  # type: ignore[arg-type]
                ingested_at=ingested_at,
                source="fred.alfred.release_vintage",
            )
        )
        reset_date = vintage_date + timedelta(days=1)  # type: ignore[operator]
        features.append(
            _feature(
                name="tier_a_event_state",
                value=0.0,
                unit="binary_24h_post_release_window",
                series_id="TIER_A",
                observation_date=reset_date,
                vintage_date=reset_date,
                ingested_at=ingested_at,
                source="fred.alfred.release_vintage",
            )
        )
    return features


class FredAlfredPITStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS fred_alfred_responses(
                    response_id TEXT PRIMARY KEY,
                    series_id TEXT NOT NULL,
                    output_type INTEGER NOT NULL,
                    request_descriptor TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    raw_response_path TEXT NOT NULL,
                    UNIQUE(series_id,output_type,request_descriptor,content_sha256)
                );
                CREATE TABLE IF NOT EXISTS fred_alfred_vintages(
                    series_id TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    vintage_date TEXT NOT NULL,
                    value REAL NOT NULL,
                    version_kind TEXT NOT NULL,
                    response_id TEXT NOT NULL,
                    PRIMARY KEY(series_id,observation_date,vintage_date)
                );
                CREATE TABLE IF NOT EXISTS macro_pit_observations(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    series_id TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    vintage_date TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_macro_pit_name_available
                    ON macro_pit_observations(name,available_at,sequence);
                """
            )

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def append(
        self,
        *,
        responses: Sequence[FREDResponseEvidence],
        versions: Sequence[Mapping[str, object]],
        features: Sequence[Mapping[str, object]],
    ) -> None:
        with self.connect() as connection:
            for item in responses:
                record = asdict(item)
                connection.execute(
                    """INSERT OR IGNORE INTO fred_alfred_responses(
                           response_id,series_id,output_type,request_descriptor,
                           requested_at,received_at,http_status,content_length,
                           content_sha256,row_count,raw_response_path
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        record["response_id"],
                        record["series_id"],
                        record["output_type"],
                        record["request_descriptor"],
                        record["requested_at"],
                        record["received_at"],
                        record["http_status"],
                        record["content_length"],
                        record["content_sha256"],
                        record["row_count"],
                        record["raw_response_path"],
                    ),
                )
            for row in versions:
                response_id = str(row["response_id"])
                existing = connection.execute(
                    """SELECT value FROM fred_alfred_vintages
                         WHERE series_id=? AND observation_date=? AND vintage_date=?""",
                    (
                        row["series_id"],
                        row["observation_date"].isoformat(),
                        row["vintage_date"].isoformat(),
                    ),
                ).fetchone()
                if existing is not None and not math.isclose(
                    float(existing["value"]), float(row["value"]), rel_tol=0, abs_tol=1e-12
                ):
                    raise ValueError("ALFRED vintage value conflicts with append-only history")
                connection.execute(
                    """INSERT OR IGNORE INTO fred_alfred_vintages(
                           series_id,observation_date,vintage_date,value,version_kind,response_id
                       ) VALUES (?,?,?,?,?,?)""",
                    (
                        row["series_id"],
                        row["observation_date"].isoformat(),
                        row["vintage_date"].isoformat(),
                        float(row["value"]),
                        row["version_kind"],
                        response_id,
                    ),
                )
            for row in features:
                if not row["event_time"] <= row["available_at"] <= row["ingested_at"]:
                    raise ValueError("macro PIT chronology violation")
                connection.execute(
                    """INSERT OR IGNORE INTO macro_pit_observations(
                           observation_id,name,value,unit,event_time,available_at,
                           ingested_at,source,series_id,observation_date,vintage_date
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        row["observation_id"],
                        row["name"],
                        float(row["value"]),
                        row["unit"],
                        _iso(row["event_time"]),
                        _iso(row["available_at"]),
                        _iso(row["ingested_at"]),
                        row["source"],
                        row["series_id"],
                        row["observation_date"],
                        row["vintage_date"],
                    ),
                )
            connection.commit()


def backfill_fred_alfred_pit(
    store: FredAlfredPITStore,
    *,
    cache_dir: Path,
    api_key: str,
    observation_start: date,
    observation_end: date,
    requester: Requester = _default_request,
    timeout_sec: float = 90.0,
) -> dict[str, object]:
    if observation_end < observation_start:
        raise ValueError("observation end precedes start")
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    responses: list[FREDResponseEvidence] = []
    versions: list[dict[str, object]] = []
    latest_ingested_at = datetime.min.replace(tzinfo=timezone.utc)
    work = [
        *((series, 4) for series in INITIAL_SERIES),
        *((series, 3) for series in REVISION_SERIES),
    ]
    for series_id, output_type in work:
        max_days = (
            MAX_DAILY_REALTIME_WINDOW_DAYS
            if output_type == 4 and series_id in DAILY_INITIAL_SERIES
            else None
        )
        for realtime_start, realtime_end in _realtime_windows(
            observation_start,
            observation_end,
            max_days=max_days,
        ):
            url, descriptor = _request_url(
                series_id=series_id,
                api_key=api_key,
                output_type=output_type,
                observation_start=observation_start,
                observation_end=observation_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
            )
            response = requester(url, timeout_sec)
            if response.http_status != 200:
                raise RuntimeError(f"FRED API returned HTTP {response.http_status}")
            latest_ingested_at = max(latest_ingested_at, response.received_at)
            content_sha256 = _sha256(response.body)
            payload = json.loads(response.body)
            parsed = (
                _parse_initial(series_id, payload)
                if output_type == 4
                else _parse_revisions(series_id, payload)
            )
            raw_path = (
                cache_dir
                / f"{series_id}.output{output_type}.{content_sha256[:16]}.json"
            )
            temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
            temporary.write_bytes(response.body)
            temporary.replace(raw_path)
            response_id = hashlib.sha256(
                f"{series_id}|{output_type}|{descriptor}|{content_sha256}".encode()
            ).hexdigest()[:32]
            evidence = FREDResponseEvidence(
                response_id=response_id,
                series_id=series_id,
                output_type=output_type,
                request_descriptor=descriptor,
                requested_at=_iso(response.requested_at),
                received_at=_iso(response.received_at),
                http_status=response.http_status,
                content_length=len(response.body),
                content_sha256=content_sha256,
                row_count=len(parsed),
                raw_response_path=str(raw_path.resolve()),
            )
            responses.append(evidence)
            for row in parsed:
                row["output_type"] = output_type
                row["response_id"] = response_id
                versions.append(row)

    # Output type 3 contains initial values as well as revisions. Prefer the
    # explicit output type 4 initial-release rows and deduplicate exact keys.
    unique: dict[tuple[str, date, date], dict[str, object]] = {}
    for row in sorted(versions, key=lambda item: int(item["output_type"])):
        key = (
            str(row["series_id"]),
            row["observation_date"],  # type: ignore[index]
            row["vintage_date"],  # type: ignore[index]
        )
        unique[key] = row
    normalized_versions = list(unique.values())
    features = _derive_features(normalized_versions, ingested_at=latest_ingested_at)
    store.append(
        responses=responses,
        versions=normalized_versions,
        features=features,
    )
    return {
        "schema_version": "fred-alfred-pit-backfill.v1",
        "status": "PASS",
        "source": "Federal Reserve Bank of St. Louis FRED/ALFRED API",
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "response_count": len(responses),
        "vintage_row_count": len(normalized_versions),
        "feature_observation_count": len(features),
        "feature_names": sorted({str(item["name"]) for item in features}),
        "responses": [asdict(item) for item in responses],
        "api_key_recorded": False,
        "pit_policy": (
            "available_at=max(observation_date,ALFRED realtime_start/vintage date) "
            "at 23:59:59 UTC"
        ),
        "current_snapshot_substitution": False,
    }


__all__: Sequence[str] = (
    "FEATURE_NAMES",
    "FredAlfredPITStore",
    "HTTPPayload",
    "backfill_fred_alfred_pit",
)
