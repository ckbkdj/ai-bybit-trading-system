from __future__ import annotations

import hashlib
import html
import json
import math
import re
import sqlite3
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

from core.providers.coinmetrics_stablecoin_pit import CoinMetricsStablecoinPITStore


SITEMAP_URL = "https://coinshares.com/sitemap/sitemap-articles__us.xml"
FEATURE_NAME = "digital_asset_fund_flow_weekly_usd"
PARSER_VERSION = "coinshares-weekly-flow-parser.v4"


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
class CoinSharesResponseEvidence:
    response_id: str
    canonical_url: str
    request_descriptor: str
    requested_at: str
    received_at: str
    http_status: int
    content_length: int
    content_sha256: str
    raw_response_path: str
    published_date: str | None
    parse_status: str
    parse_detail: str | None


Requester = Callable[[str, float], HTTPPayload]


def _default_request(url: str, timeout_sec: float) -> HTTPPayload:
    requested_at = datetime.now(timezone.utc)
    try:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "ai-bot3-coinshares-fund-flow-pit/1"},
        )
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read()
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"CoinShares returned HTTP {exc.code}") from None
    except Exception as exc:
        raise RuntimeError(f"CoinShares request failed: {type(exc).__name__}") from None
    return HTTPPayload(
        body=body,
        requested_at=requested_at,
        received_at=datetime.now(timezone.utc),
        http_status=status,
    )


def _article_urls(body: bytes) -> list[str]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise ValueError("CoinShares sitemap is invalid XML") from exc
    urls = sorted(
        {
            str(element.text).strip()
            for element in root.iter()
            if element.tag.endswith("loc")
            and element.text
            and "/insights/research-data/fund-flows-" in str(element.text)
        }
    )
    if not urls:
        raise ValueError("CoinShares sitemap contains no fund-flow articles")
    return urls


def _plain_text(body: bytes) -> str:
    decoded = body.decode("utf-8", errors="replace")
    decoded = re.sub(r"<script\b[^>]*>.*?</script>", " ", decoded, flags=re.I | re.S)
    decoded = re.sub(r"<style\b[^>]*>.*?</style>", " ", decoded, flags=re.I | re.S)
    decoded = re.sub(
        r"</(?:h[1-6]|p|li|div|section|article)\s*>",
        ". ",
        decoded,
        flags=re.I,
    )
    decoded = re.sub(r"<[^>]+>", " ", decoded)
    return re.sub(r"\s+", " ", html.unescape(decoded)).strip()


def _published_date(body: bytes) -> date:
    decoded = body.decode("utf-8", errors="replace")
    match = re.search(
        r'class="published-on"[^>]*>\s*Published on\s*<span>([^<]+)</span>',
        decoded,
        flags=re.I,
    )
    if not match:
        raise ValueError("CoinShares article has no explicit Published on date")
    value = re.sub(r"(?<=\d)(st|nd|rd|th)", "", html.unescape(match.group(1)), flags=re.I)
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"^Sept\b", "Sep", value, flags=re.I)
    for pattern in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    raise ValueError("CoinShares Published on date is not recognized")


def _scaled_usd(number: str, scale: str) -> float:
    normalized = number
    if "," in normalized and "." not in normalized:
        tail = normalized.rsplit(",", 1)[-1]
        normalized = (
            normalized.replace(",", ".")
            if len(tail) in {1, 2}
            else normalized.replace(",", "")
        )
    else:
        normalized = normalized.replace(",", "")
    value = float(normalized)
    multipliers = {
        "m": 1_000_000.0,
        "mn": 1_000_000.0,
        "million": 1_000_000.0,
        "b": 1_000_000_000.0,
        "bn": 1_000_000_000.0,
        "billion": 1_000_000_000.0,
    }
    result = value * multipliers[scale.lower()]
    if not math.isfinite(result) or result < 0:
        raise ValueError("CoinShares fund-flow amount is invalid")
    return result


_DIRECTION_FIRST = re.compile(
    r"\b(?P<direction>inflows?|outflows?)\b[^.!?;$]{0,100}?"
    r"(?:US\s*)?\$\s*(?P<number>[0-9][0-9,.]*)\s*"
    r"(?P<scale>bn|billion|million|mn|m|b)\b",
    flags=re.I,
)
_AMOUNT_FIRST = re.compile(
    r"(?:US\s*)?\$\s*(?P<number>[0-9][0-9,.]*)\s*"
    r"(?P<scale>bn|billion|million|mn|m|b)\b[^.!?;$]{0,45}?"
    r"\b(?P<direction>inflows?|outflows?)\b",
    flags=re.I,
)


def _weekly_flow(body: bytes) -> tuple[float, str]:
    text = _plain_text(body)
    sentences = re.split(r"(?<=[.!?])\s+", text)
    candidates: list[tuple[int, float, str]] = []
    for position, sentence in enumerate(sentences):
        lowered = sentence.lower()
        if "digital asset" not in lowered:
            continue
        if not any(term in lowered for term in ("product", "etp", "fund")):
            continue
        matches = list(_DIRECTION_FIRST.finditer(sentence)) + list(
            _AMOUNT_FIRST.finditer(sentence)
        )
        if not matches:
            continue
        first_amount = min(match.start("number") for match in matches)
        matches = [
            match for match in matches if match.start("number") == first_amount
        ]
        match = min(
            matches,
            key=lambda item: abs(
                item.start("number") - item.start("direction")
            ),
        )
        amount = _scaled_usd(match.group("number"), match.group("scale"))
        if match.group("direction").lower().startswith("outflow"):
            amount = -amount
        priority = 0
        if "investment products" in lowered:
            priority += 4
        if any(term in lowered for term in ("last week", "weekly", "week of")):
            priority += 2
        if any(term in lowered for term in ("totalling", "totaling", "recorded", "saw")):
            priority += 1
        candidates.append((priority * 1_000_000 - position, amount, sentence))
    if not candidates:
        raise ValueError("CoinShares article has no parsable global weekly flow")
    best_priority = max(item[0] for item in candidates)
    best = [item for item in candidates if item[0] == best_priority]
    values = {item[1] for item in best}
    if len(values) != 1:
        raise ValueError("CoinShares article has conflicting top-priority flow values")
    _, value, sentence = best[0]
    lowered = sentence.lower()
    if not any(term in lowered for term in ("week", "weekly")) and re.search(
        r"\b(?:finished|full[- ]year|annual)\b.{0,30}\b20\d{2}\b",
        lowered,
    ):
        raise ValueError("CoinShares publication is an annual aggregate, not a weekly flow")
    return value, sentence[:500]


class CoinSharesFundFlowPITStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        CoinMetricsStablecoinPITStore(self.path)
        with self.connect() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS coinshares_responses(
                       response_id TEXT PRIMARY KEY,
                       canonical_url TEXT NOT NULL,
                       request_descriptor TEXT NOT NULL,
                       requested_at TEXT NOT NULL,
                       received_at TEXT NOT NULL,
                       http_status INTEGER NOT NULL,
                       content_length INTEGER NOT NULL,
                       content_sha256 TEXT NOT NULL,
                       raw_response_path TEXT NOT NULL,
                       published_date TEXT,
                       parse_status TEXT NOT NULL,
                       parse_detail TEXT,
                       UNIQUE(canonical_url,content_sha256)
                   )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS coinshares_parse_attempts(
                       sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                       response_id TEXT NOT NULL,
                       parser_version TEXT NOT NULL,
                       parse_status TEXT NOT NULL,
                       parse_detail TEXT,
                       recorded_at TEXT NOT NULL,
                       UNIQUE(response_id,parser_version,parse_status,parse_detail)
                   )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS flow_pit_observation_invalidations(
                       observation_id TEXT PRIMARY KEY,
                       invalidated_at TEXT NOT NULL,
                       reason TEXT NOT NULL,
                       parser_version TEXT NOT NULL
                   )"""
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
        responses: Sequence[CoinSharesResponseEvidence],
        features: Sequence[Mapping[str, object]],
        invalidations: Sequence[tuple[date, str]] = (),
    ) -> None:
        with self.connect() as connection:
            for response in responses:
                record = asdict(response)
                connection.execute(
                    """INSERT OR IGNORE INTO coinshares_responses(
                           response_id,canonical_url,request_descriptor,
                           requested_at,received_at,http_status,content_length,
                           content_sha256,raw_response_path,published_date,
                           parse_status,parse_detail
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    tuple(record[key] for key in (
                        "response_id",
                        "canonical_url",
                        "request_descriptor",
                        "requested_at",
                        "received_at",
                        "http_status",
                        "content_length",
                        "content_sha256",
                        "raw_response_path",
                        "published_date",
                        "parse_status",
                        "parse_detail",
                    )),
                )
                connection.execute(
                    """INSERT OR IGNORE INTO coinshares_parse_attempts(
                           response_id,parser_version,parse_status,parse_detail,
                           recorded_at
                       ) VALUES (?,?,?,?,?)""",
                    (
                        record["response_id"],
                        PARSER_VERSION,
                        record["parse_status"],
                        record["parse_detail"],
                        _iso(datetime.now(timezone.utc)),
                    ),
                )
            for row in features:
                if not row["event_time"] <= row["available_at"] <= row["ingested_at"]:
                    raise ValueError("fund-flow PIT chronology violation")
                existing = connection.execute(
                    "SELECT value FROM flow_pit_observations WHERE observation_id=?",
                    (row["observation_id"],),
                ).fetchone()
                if existing is not None and not math.isclose(
                    float(existing["value"]),
                    float(row["value"]),
                    rel_tol=0,
                    abs_tol=1e-6,
                ):
                    raise ValueError(
                        "CoinShares historical publication value changed; preserve "
                        "the existing PIT store and investigate the source revision"
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
            for published, reason in invalidations:
                rows = connection.execute(
                    """SELECT observation_id FROM flow_pit_observations
                         WHERE name=? AND observation_date=?""",
                    (FEATURE_NAME, published.isoformat()),
                ).fetchall()
                for row in rows:
                    connection.execute(
                        """INSERT OR IGNORE INTO flow_pit_observation_invalidations(
                               observation_id,invalidated_at,reason,parser_version
                           ) VALUES (?,?,?,?)""",
                        (
                            row["observation_id"],
                            _iso(datetime.now(timezone.utc)),
                            reason,
                            PARSER_VERSION,
                        ),
                    )
            connection.commit()


def _raw_path(cache_dir: Path, url: str, content_sha256: str) -> Path:
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", slug)[:100]
    return cache_dir / f"{slug}.{content_sha256[:16]}.html"


def backfill_coinshares_fund_flow_pit(
    store: CoinSharesFundFlowPITStore,
    *,
    cache_dir: Path,
    publication_start: date,
    publication_end: date,
    timeout_sec: float = 60.0,
    workers: int = 4,
    requester: Requester = _default_request,
) -> dict[str, object]:
    if publication_end < publication_start:
        raise ValueError("CoinShares publication end precedes start")
    if not 1 <= workers <= 8:
        raise ValueError("CoinShares workers must be between 1 and 8")
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    sitemap = requester(SITEMAP_URL, timeout_sec)
    if sitemap.http_status != 200:
        raise RuntimeError(f"CoinShares sitemap returned HTTP {sitemap.http_status}")
    sitemap_sha = _sha256(sitemap.body)
    sitemap_path = cache_dir / f"sitemap-articles-us.{sitemap_sha[:16]}.xml"
    temporary = sitemap_path.with_suffix(sitemap_path.suffix + ".tmp")
    temporary.write_bytes(sitemap.body)
    temporary.replace(sitemap_path)
    urls = _article_urls(sitemap.body)

    def fetch(url: str) -> tuple[str, HTTPPayload | Exception]:
        try:
            return url, requester(url, timeout_sec)
        except Exception as exc:  # captured as evidence in the report
            return url, exc

    with ThreadPoolExecutor(max_workers=workers) as executor:
        fetched = list(executor.map(fetch, urls))

    responses: list[CoinSharesResponseEvidence] = []
    features: list[dict[str, object]] = []
    exclusions: list[dict[str, str]] = []
    invalidations: list[tuple[date, str]] = []
    duplicate_dates: dict[date, float] = {}
    for url, result in fetched:
        if isinstance(result, Exception):
            exclusions.append(
                {"url": url, "reason": f"REQUEST_FAILED:{type(result).__name__}"}
            )
            continue
        content_sha256 = _sha256(result.body)
        raw_path = _raw_path(cache_dir, url, content_sha256)
        temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
        temporary.write_bytes(result.body)
        temporary.replace(raw_path)
        response_id = hashlib.sha256(f"{url}|{content_sha256}".encode()).hexdigest()[:32]
        published: date | None = None
        parse_status = "EXCLUDED"
        parse_detail: str | None = None
        feature: dict[str, object] | None = None
        try:
            if result.http_status != 200:
                raise ValueError(f"HTTP_{result.http_status}")
            published = _published_date(result.body)
            if not publication_start <= published <= publication_end:
                parse_status = "OUTSIDE_WINDOW"
                parse_detail = published.isoformat()
            else:
                value, sentence = _weekly_flow(result.body)
                prior = duplicate_dates.get(published)
                if prior is not None and not math.isclose(
                    prior, value, rel_tol=0, abs_tol=1e-6
                ):
                    raise ValueError("conflicting articles share a publication date")
                duplicate_dates[published] = value
                event_time = datetime.combine(published, time(), timezone.utc)
                available_at = datetime.combine(
                    published, time(23, 59, 59), timezone.utc
                )
                if available_at > result.received_at:
                    raise ValueError("publication availability exceeds fetch time")
                feature = {
                    "observation_id": hashlib.sha256(
                        f"{FEATURE_NAME}|{published}|{url}|{PARSER_VERSION}".encode()
                    ).hexdigest()[:32],
                    "name": FEATURE_NAME,
                    "value": value,
                    "unit": "usd",
                    "event_time": event_time,
                    "available_at": available_at,
                    "ingested_at": result.received_at,
                    "source": "coinshares.official.weekly_publication",
                    "series_id": "COINSHARES.DIGITAL_ASSET_FUND_FLOWS",
                    "observation_date": published.isoformat(),
                    "response_id": response_id,
                }
                parse_status = "PARSED"
                parse_detail = sentence
        except Exception as exc:
            parse_detail = f"{type(exc).__name__}:{exc}"
            exclusions.append({"url": url, "reason": parse_detail})
            if published is not None and publication_start <= published <= publication_end:
                invalidations.append((published, parse_detail))
        responses.append(
            CoinSharesResponseEvidence(
                response_id=response_id,
                canonical_url=url,
                request_descriptor=json.dumps(
                    {"url": url}, sort_keys=True, separators=(",", ":")
                ),
                requested_at=_iso(result.requested_at),
                received_at=_iso(result.received_at),
                http_status=result.http_status,
                content_length=len(result.body),
                content_sha256=content_sha256,
                raw_response_path=str(raw_path.resolve()),
                published_date=published.isoformat() if published else None,
                parse_status=parse_status,
                parse_detail=parse_detail,
            )
        )
        if feature is not None:
            features.append(feature)
    features.sort(key=lambda item: (item["available_at"], item["observation_id"]))
    if len(features) < 52:
        raise RuntimeError("fewer than 52 CoinShares weekly publications were parsed")
    parsed_dates = sorted(
        date.fromisoformat(str(item["observation_date"])) for item in features
    )
    store.append(
        responses=responses,
        features=features,
        invalidations=invalidations,
    )
    return {
        "schema_version": "coinshares-fund-flow-pit-backfill.v1",
        "status": "PASS" if not exclusions else "PASS_WITH_EXCLUSIONS",
        "source": "CoinShares official US research publications",
        "sitemap_url": SITEMAP_URL,
        "sitemap_content_sha256": sitemap_sha,
        "sitemap_raw_response_path": str(sitemap_path.resolve()),
        "sitemap_article_count": len(urls),
        "publication_start": publication_start.isoformat(),
        "publication_end": publication_end.isoformat(),
        "article_response_count": len(responses),
        "feature_observation_count": len(features),
        "feature_name": FEATURE_NAME,
        "feature_start": parsed_dates[0].isoformat(),
        "feature_end": parsed_dates[-1].isoformat(),
        "excluded_count": len(exclusions),
        "exclusions": exclusions,
        "pit_policy": (
            "available_at=explicit official Published on date at 23:59:59 UTC; "
            "raw article and sitemap bodies frozen; later value conflicts fail closed"
        ),
        "semantic_scope": (
            "global weekly digital-asset investment-product net flow; not daily "
            "issuer-level ETF creations/redemptions"
        ),
    }


__all__: Sequence[str] = (
    "FEATURE_NAME",
    "CoinSharesFundFlowPITStore",
    "HTTPPayload",
    "backfill_coinshares_fund_flow_pit",
)
