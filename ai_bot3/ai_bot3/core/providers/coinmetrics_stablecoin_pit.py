from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence


API_ENDPOINT = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
ASSETS: tuple[str, ...] = ("usdc", "usdt")
METRICS: tuple[str, ...] = ("SplyCur",)
FEATURE_NAMES: tuple[str, ...] = (
    "stablecoin_net_issuance_1d_usd",
    "stablecoin_net_issuance_7d_usd",
    "stablecoin_supply_change_7d_ratio",
)
PUBLICATION_LAG = timedelta(days=2)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class HTTPPayload:
    body: bytes
    requested_at: datetime
    received_at: datetime
    http_status: int


@dataclass(frozen=True)
class CoinMetricsResponseEvidence:
    response_id: str
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
            headers={"User-Agent": "ai-bot3-coinmetrics-stablecoin-pit/1"},
        )
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read()
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Coin Metrics API returned HTTP {exc.code}") from None
    except Exception as exc:
        raise RuntimeError(
            f"Coin Metrics API request failed: {type(exc).__name__}"
        ) from None
    return HTTPPayload(
        body=body,
        requested_at=requested_at,
        received_at=datetime.now(timezone.utc),
        http_status=status,
    )


def _request_url(start: date, end: date) -> tuple[str, str]:
    if end < start:
        raise ValueError("Coin Metrics end date precedes start date")
    parameters = {
        "assets": ",".join(ASSETS),
        "metrics": ",".join(METRICS),
        "frequency": "1d",
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "status": "reviewed",
        "page_size": 10_000,
    }
    descriptor = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
    return API_ENDPOINT + "?" + urllib.parse.urlencode(parameters), descriptor


def _number(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError("Coin Metrics supply value is not numeric") from None
    if not math.isfinite(result) or result < 0:
        raise ValueError("Coin Metrics supply value is invalid")
    return result


def _parse_rows(payload: Mapping[str, object]) -> dict[date, float]:
    raw_rows = payload.get("data")
    if not isinstance(raw_rows, list):
        raise ValueError("Coin Metrics response has no data rows")
    if payload.get("next_page_url"):
        raise ValueError("Coin Metrics response was unexpectedly paginated")
    by_date: dict[date, dict[str, float]] = {}
    for item in raw_rows:
        if not isinstance(item, Mapping):
            continue
        asset = str(item.get("asset", "")).lower()
        if asset not in ASSETS or "SplyCur" not in item:
            continue
        observed = date.fromisoformat(str(item.get("time", ""))[:10])
        asset_rows = by_date.setdefault(observed, {})
        value = _number(item["SplyCur"])
        if asset in asset_rows and not math.isclose(
            asset_rows[asset], value, rel_tol=0, abs_tol=1e-6
        ):
            raise ValueError("Coin Metrics response contains conflicting rows")
        asset_rows[asset] = value
    complete = {
        observed: sum(asset_rows[asset] for asset in ASSETS)
        for observed, asset_rows in by_date.items()
        if set(asset_rows) == set(ASSETS)
    }
    if not complete:
        raise ValueError("Coin Metrics response has no complete USDC/USDT dates")
    return complete


def _derive_features(
    supply_by_date: Mapping[date, float],
    *,
    ingested_at: datetime,
    response_id: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for observed in sorted(supply_by_date):
        current = float(supply_by_date[observed])
        one_day = supply_by_date.get(observed - timedelta(days=1))
        seven_day = supply_by_date.get(observed - timedelta(days=7))
        event_time = datetime.combine(observed, datetime.min.time(), timezone.utc)
        available_at = event_time + PUBLICATION_LAG
        if available_at > ingested_at:
            continue
        values: dict[str, float] = {}
        if one_day is not None:
            values["stablecoin_net_issuance_1d_usd"] = current - float(one_day)
        if seven_day is not None:
            values["stablecoin_net_issuance_7d_usd"] = current - float(seven_day)
            if float(seven_day) > 0:
                values["stablecoin_supply_change_7d_ratio"] = (
                    current / float(seven_day) - 1.0
                )
        for name, value in values.items():
            unit = "ratio" if name.endswith("_ratio") else "usd"
            observation_id = hashlib.sha256(
                f"{name}|{observed}|coinmetrics-ledger-v1".encode()
            ).hexdigest()[:32]
            rows.append(
                {
                    "observation_id": observation_id,
                    "name": name,
                    "value": value,
                    "unit": unit,
                    "event_time": event_time,
                    "available_at": available_at,
                    "ingested_at": ingested_at,
                    "source": "coinmetrics.community.ledger_reconstruction",
                    "series_id": "USDC+USDT.SplyCur",
                    "observation_date": observed.isoformat(),
                    "response_id": response_id,
                }
            )
    return rows


class CoinMetricsStablecoinPITStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS coinmetrics_responses(
                    response_id TEXT PRIMARY KEY,
                    request_descriptor TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    raw_response_path TEXT NOT NULL,
                    UNIQUE(request_descriptor,content_sha256)
                );
                CREATE TABLE IF NOT EXISTS flow_pit_observations(
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
                    response_id TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_flow_pit_name_available
                    ON flow_pit_observations(name,available_at,sequence);
                CREATE TABLE IF NOT EXISTS flow_pit_observation_invalidations(
                    observation_id TEXT PRIMARY KEY,
                    invalidated_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    parser_version TEXT NOT NULL
                );
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
        response: CoinMetricsResponseEvidence,
        features: Sequence[Mapping[str, object]],
    ) -> None:
        with self.connect() as connection:
            evidence = asdict(response)
            connection.execute(
                """INSERT OR IGNORE INTO coinmetrics_responses(
                       response_id,request_descriptor,requested_at,received_at,
                       http_status,content_length,content_sha256,row_count,
                       raw_response_path
                   ) VALUES (?,?,?,?,?,?,?,?,?)""",
                tuple(evidence[key] for key in (
                    "response_id",
                    "request_descriptor",
                    "requested_at",
                    "received_at",
                    "http_status",
                    "content_length",
                    "content_sha256",
                    "row_count",
                    "raw_response_path",
                )),
            )
            for row in features:
                if not row["event_time"] <= row["available_at"] <= row["ingested_at"]:
                    raise ValueError("stablecoin PIT chronology violation")
                existing = connection.execute(
                    """SELECT value FROM flow_pit_observations
                         WHERE observation_id=?""",
                    (row["observation_id"],),
                ).fetchone()
                if existing is not None and not math.isclose(
                    float(existing["value"]),
                    float(row["value"]),
                    rel_tol=0,
                    abs_tol=1e-6,
                ):
                    raise ValueError(
                        "Coin Metrics historical ledger value changed; preserve the "
                        "existing PIT store and investigate the source revision"
                    )
                connection.execute(
                    """INSERT OR IGNORE INTO flow_pit_observations(
                           observation_id,name,value,unit,event_time,available_at,
                           ingested_at,source,series_id,observation_date,response_id
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
                        row["response_id"],
                    ),
                )
            connection.commit()


def backfill_coinmetrics_stablecoin_pit(
    store: CoinMetricsStablecoinPITStore,
    *,
    cache_dir: Path,
    observation_start: date,
    observation_end: date,
    timeout_sec: float = 60.0,
    requester: Requester = _default_request,
) -> dict[str, object]:
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    url, descriptor = _request_url(observation_start, observation_end)
    response = requester(url, timeout_sec)
    if response.http_status != 200:
        raise RuntimeError(f"Coin Metrics API returned HTTP {response.http_status}")
    payload = json.loads(response.body)
    if not isinstance(payload, Mapping):
        raise ValueError("Coin Metrics response is not an object")
    supply = _parse_rows(payload)
    content_sha256 = _sha256(response.body)
    raw_path = cache_dir / f"stablecoin-supply.{content_sha256[:16]}.json"
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    temporary.write_bytes(response.body)
    temporary.replace(raw_path)
    response_id = hashlib.sha256(
        f"{descriptor}|{content_sha256}".encode()
    ).hexdigest()[:32]
    evidence = CoinMetricsResponseEvidence(
        response_id=response_id,
        request_descriptor=descriptor,
        requested_at=_iso(response.requested_at),
        received_at=_iso(response.received_at),
        http_status=response.http_status,
        content_length=len(response.body),
        content_sha256=content_sha256,
        row_count=len(supply) * len(ASSETS),
        raw_response_path=str(raw_path.resolve()),
    )
    features = _derive_features(
        supply,
        ingested_at=response.received_at,
        response_id=response_id,
    )
    store.append(response=evidence, features=features)
    return {
        "schema_version": "coinmetrics-stablecoin-pit-backfill.v1",
        "status": "PASS",
        "source": "Coin Metrics Community API",
        "source_license": "Creative Commons community data; verify production use terms",
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "response": asdict(evidence),
        "source_row_count": evidence.row_count,
        "feature_observation_count": len(features),
        "feature_names": sorted({str(item["name"]) for item in features}),
        "api_key_required": False,
        "pit_policy": (
            "immutable-ledger SplyCur reconstruction requested as reviewed; "
            "available_at=metric day + 48h; raw response frozen and later "
            "historical conflicts fail closed"
        ),
        "semantic_scope": (
            "USDC+USDT net issuance/redemption; not exchange netflow and not ETF flow"
        ),
    }


__all__: Sequence[str] = (
    "FEATURE_NAMES",
    "CoinMetricsStablecoinPITStore",
    "HTTPPayload",
    "backfill_coinmetrics_stablecoin_pit",
)
