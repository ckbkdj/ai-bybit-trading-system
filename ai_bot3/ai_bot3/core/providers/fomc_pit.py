from __future__ import annotations

import hashlib
import html
import json
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Mapping, Sequence


FEDERAL_RESERVE_ORIGIN = "https://www.federalreserve.gov"
FOMC_INDEX_URL = FEDERAL_RESERVE_ORIGIN + "/newsevents/pressreleases/{year}-press-fomc.htm"
GENERAL_INDEX_URL = FEDERAL_RESERVE_ORIGIN + "/newsevents/pressreleases/{year}-press.htm"
FOMC_FEATURE_NAME = "fomc_statement_event_state"
FOMC_FEATURE_SOURCE = "federal_reserve.fomc_statement"
_STATEMENT_PATH = re.compile(
    r"^/newsevents/pressreleases/monetary(?P<day>\d{8})a\.htm$",
    re.IGNORECASE,
)
_RELEASE_TIME = re.compile(
    r"For\s+release\s+at\s+"
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*"
    r"(?P<meridiem>[ap])\.?m\.?\s*(?P<zone>EST|EDT)",
    re.IGNORECASE,
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _canonical(payload: object) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()


@dataclass(frozen=True)
class HTTPPayload:
    body: bytes
    requested_at: datetime
    received_at: datetime
    http_status: int


@dataclass(frozen=True)
class OfficialMacroResponseEvidence:
    response_id: str
    source: str
    document_kind: str
    request_url: str
    requested_at: str
    received_at: str
    http_status: int
    content_length: int
    content_sha256: str
    row_count: int
    raw_response_path: str


@dataclass(frozen=True)
class FOMCStatementEvent:
    event_id: str
    statement_url: str
    title: str
    released_at: str
    response_id: str
    content_sha256: str


Requester = Callable[[str, float], HTTPPayload]


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._href: str | None = None
        self._text: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a" or self._href is not None:
            return
        href = dict(attrs).get("href")
        if href:
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, " ".join(self._text)))
            self._href = None
            self._text = []


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []

    def handle_data(self, data: str) -> None:
        self.text.append(data)


def _default_request(url: str, timeout_sec: float) -> HTTPPayload:
    requested_at = datetime.now(timezone.utc)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": "ai-bot3-research/1.0 (strict PIT FOMC evidence)",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read()
            status = int(getattr(response, "status", 200))
    except urllib.error.HTTPError as exc:
        body = exc.read()
        status = int(exc.code)
    except Exception as exc:
        raise RuntimeError(f"Federal Reserve request failed for {url}") from exc
    return HTTPPayload(
        body=body,
        requested_at=requested_at,
        received_at=datetime.now(timezone.utc),
        http_status=status,
    )


def _visible_text(body: bytes) -> str:
    parser = _TextParser()
    parser.feed(body.decode("utf-8", errors="replace"))
    return " ".join(html.unescape(part).strip() for part in parser.text if part.strip())


def _statement_urls(body: bytes, *, year: int) -> list[str]:
    parser = _LinkParser()
    parser.feed(body.decode("utf-8", errors="replace"))
    result: dict[str, str] = {}
    for href, label in parser.links:
        normalized_label = " ".join(label.split()).lower()
        absolute = urllib.parse.urljoin(FEDERAL_RESERVE_ORIGIN, href)
        path = urllib.parse.urlsplit(absolute).path
        match = _STATEMENT_PATH.fullmatch(path)
        if not match or int(match.group("day")[:4]) != int(year):
            continue
        if "fomc statement" not in normalized_label or "minutes" in normalized_label:
            continue
        result[path.lower()] = absolute
    return [result[key] for key in sorted(result)]


def _release_datetime(statement_url: str, body: bytes) -> datetime:
    path = urllib.parse.urlsplit(statement_url).path
    match = _STATEMENT_PATH.fullmatch(path)
    if match is None:
        raise ValueError(f"not a canonical FOMC statement URL: {statement_url}")
    release_day = datetime.strptime(match.group("day"), "%Y%m%d").date()
    release_match = _RELEASE_TIME.search(_visible_text(body))
    if release_match is None:
        raise ValueError(f"official FOMC page has no explicit release time: {statement_url}")
    hour = int(release_match.group("hour"))
    minute = int(release_match.group("minute") or 0)
    if not 1 <= hour <= 12 or not 0 <= minute <= 59:
        raise ValueError(f"official FOMC page has an invalid release time: {statement_url}")
    if release_match.group("meridiem").lower() == "p" and hour != 12:
        hour += 12
    elif release_match.group("meridiem").lower() == "a" and hour == 12:
        hour = 0
    offset_hours = -5 if release_match.group("zone").upper() == "EST" else -4
    local = datetime(
        release_day.year,
        release_day.month,
        release_day.day,
        hour,
        minute,
        tzinfo=timezone(timedelta(hours=offset_hours)),
    )
    return local.astimezone(timezone.utc)


def _statement_title(body: bytes) -> str:
    text = _visible_text(body)
    match = re.search(r"Federal Reserve issues FOMC statement", text, re.IGNORECASE)
    return match.group(0) if match else "Federal Reserve FOMC statement"


def _write_raw(cache_dir: Path, name: str, body: bytes) -> tuple[Path, str]:
    digest = _sha256(body)
    raw_path = cache_dir / f"{name}.{digest[:16]}.html"
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    temporary.write_bytes(body)
    temporary.replace(raw_path)
    return raw_path.resolve(), digest


def _response_evidence(
    *,
    source: str,
    document_kind: str,
    request_url: str,
    response: HTTPPayload,
    raw_path: Path,
    digest: str,
    row_count: int,
) -> OfficialMacroResponseEvidence:
    response_id = hashlib.sha256(
        _canonical(
            {
                "source": source,
                "document_kind": document_kind,
                "request_url": request_url,
                "content_sha256": digest,
            }
        )
    ).hexdigest()[:32]
    return OfficialMacroResponseEvidence(
        response_id=response_id,
        source=source,
        document_kind=document_kind,
        request_url=request_url,
        requested_at=_iso(response.requested_at),
        received_at=_iso(response.received_at),
        http_status=response.http_status,
        content_length=len(response.body),
        content_sha256=digest,
        row_count=int(row_count),
        raw_response_path=str(raw_path),
    )


def _state_transitions(
    events: Sequence[FOMCStatementEvent], *, ingested_at: datetime
) -> list[dict[str, object]]:
    release_times = sorted(
        datetime.fromisoformat(item.released_at.replace("Z", "+00:00"))
        for item in events
    )
    transitions: list[tuple[datetime, float]] = []
    active_until: datetime | None = None
    for released_at in release_times:
        if active_until is None or released_at > active_until:
            if active_until is not None:
                transitions.append((active_until, 0.0))
            transitions.append((released_at, 1.0))
            active_until = released_at + timedelta(hours=24)
        else:
            active_until = max(active_until, released_at + timedelta(hours=24))
    if active_until is not None:
        transitions.append((active_until, 0.0))

    features: list[dict[str, object]] = []
    for event_time, value in transitions:
        if event_time > ingested_at:
            raise ValueError("FOMC transition cannot be ingested before it is available")
        observation_id = hashlib.sha256(
            f"{FOMC_FEATURE_NAME}|{_iso(event_time)}|{value:.1f}".encode()
        ).hexdigest()[:32]
        features.append(
            {
                "observation_id": observation_id,
                "name": FOMC_FEATURE_NAME,
                "value": value,
                "unit": "binary_24h_post_release_window",
                "event_time": event_time,
                "available_at": event_time,
                "ingested_at": ingested_at,
                "source": FOMC_FEATURE_SOURCE,
                "series_id": "FOMC_STATEMENT",
                "observation_date": event_time.date().isoformat(),
                "vintage_date": event_time.date().isoformat(),
            }
        )
    return features


class FOMCPITStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS official_macro_responses(
                    response_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    document_kind TEXT NOT NULL,
                    request_url TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    raw_response_path TEXT NOT NULL,
                    UNIQUE(source,request_url,content_sha256)
                );
                CREATE TABLE IF NOT EXISTS fomc_statement_events(
                    event_id TEXT PRIMARY KEY,
                    statement_url TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    released_at TEXT NOT NULL,
                    response_id TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL
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
        responses: Sequence[OfficialMacroResponseEvidence],
        events: Sequence[FOMCStatementEvent],
        features: Sequence[Mapping[str, object]],
    ) -> None:
        with self.connect() as connection:
            for item in responses:
                row = asdict(item)
                connection.execute(
                    """INSERT OR IGNORE INTO official_macro_responses(
                           response_id,source,document_kind,request_url,requested_at,
                           received_at,http_status,content_length,content_sha256,
                           row_count,raw_response_path
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    tuple(row[column] for column in (
                        "response_id", "source", "document_kind", "request_url",
                        "requested_at", "received_at", "http_status", "content_length",
                        "content_sha256", "row_count", "raw_response_path",
                    )),
                )
            for item in events:
                existing = connection.execute(
                    "SELECT released_at FROM fomc_statement_events WHERE statement_url=?",
                    (item.statement_url,),
                ).fetchone()
                if existing is not None and str(existing["released_at"]) != item.released_at:
                    raise ValueError("official FOMC release time conflicts with stored history")
                connection.execute(
                    """INSERT OR IGNORE INTO fomc_statement_events(
                           event_id,statement_url,title,released_at,response_id,content_sha256
                       ) VALUES (?,?,?,?,?,?)""",
                    (
                        item.event_id,
                        item.statement_url,
                        item.title,
                        item.released_at,
                        item.response_id,
                        item.content_sha256,
                    ),
                )
            for row in features:
                if not row["event_time"] <= row["available_at"] <= row["ingested_at"]:
                    raise ValueError("FOMC PIT chronology violation")
                existing = connection.execute(
                    """SELECT value,source FROM macro_pit_observations
                         WHERE observation_id=?""",
                    (row["observation_id"],),
                ).fetchone()
                if existing is not None and (
                    float(existing["value"]) != float(row["value"])
                    or str(existing["source"]) != str(row["source"])
                ):
                    raise ValueError("FOMC PIT observation conflicts with stored history")
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


def backfill_fomc_pit(
    store: FOMCPITStore,
    *,
    cache_dir: Path,
    observation_start: date,
    observation_end: date,
    requester: Requester = _default_request,
    timeout_sec: float = 90.0,
) -> dict[str, object]:
    if observation_end < observation_start:
        raise ValueError("observation end precedes start")
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    responses: list[OfficialMacroResponseEvidence] = []
    events: list[FOMCStatementEvent] = []
    statement_urls: dict[str, str] = {}

    for year in range(observation_start.year, observation_end.year + 1):
        index_url = ""
        response: HTTPPayload | None = None
        for candidate in (
            FOMC_INDEX_URL.format(year=year),
            GENERAL_INDEX_URL.format(year=year),
        ):
            attempted = requester(candidate, timeout_sec)
            if attempted.http_status == 404:
                continue
            index_url = candidate
            response = attempted
            break
        if response is None:
            raise RuntimeError(f"Federal Reserve has no press release index for {year}")
        if response.http_status != 200:
            raise RuntimeError(
                f"Federal Reserve FOMC index returned HTTP {response.http_status}"
            )
        urls = _statement_urls(response.body, year=year)
        raw_path, digest = _write_raw(cache_dir, f"fomc-index-{year}", response.body)
        responses.append(
            _response_evidence(
                source=FOMC_FEATURE_SOURCE,
                document_kind="year_index",
                request_url=index_url,
                response=response,
                raw_path=raw_path,
                digest=digest,
                row_count=len(urls),
            )
        )
        for url in urls:
            path = urllib.parse.urlsplit(url).path
            match = _STATEMENT_PATH.fullmatch(path)
            if match is None:
                continue
            event_day = datetime.strptime(match.group("day"), "%Y%m%d").date()
            if observation_start <= event_day <= observation_end:
                statement_urls[path.lower()] = url

    if not statement_urls:
        raise RuntimeError("official FOMC indexes contain no statement in requested range")

    latest_ingested_at = datetime.min.replace(tzinfo=timezone.utc)
    for path, statement_url in sorted(statement_urls.items()):
        response = requester(statement_url, timeout_sec)
        if response.http_status != 200:
            raise RuntimeError(f"Federal Reserve FOMC statement returned HTTP {response.http_status}")
        released_at = _release_datetime(statement_url, response.body)
        if (
            released_at.date() < observation_start
            or released_at.date() > observation_end
        ):
            continue
        if response.received_at < released_at:
            raise RuntimeError("official FOMC response was received before its stated release time")
        statement_match = _STATEMENT_PATH.fullmatch(path)
        if statement_match is None:
            raise RuntimeError("canonical FOMC statement path changed during backfill")
        raw_name = "fomc-statement-" + statement_match.group("day")
        raw_path, digest = _write_raw(cache_dir, raw_name, response.body)
        evidence = _response_evidence(
            source=FOMC_FEATURE_SOURCE,
            document_kind="statement",
            request_url=statement_url,
            response=response,
            raw_path=raw_path,
            digest=digest,
            row_count=1,
        )
        responses.append(evidence)
        latest_ingested_at = max(latest_ingested_at, response.received_at)
        event_id = hashlib.sha256(
            f"{statement_url}|{_iso(released_at)}".encode()
        ).hexdigest()[:32]
        events.append(
            FOMCStatementEvent(
                event_id=event_id,
                statement_url=statement_url,
                title=_statement_title(response.body),
                released_at=_iso(released_at),
                response_id=evidence.response_id,
                content_sha256=digest,
            )
        )

    if not events:
        raise RuntimeError("official FOMC statement pages produced no PIT events")
    features = _state_transitions(events, ingested_at=latest_ingested_at)
    store.append(responses=responses, events=events, features=features)
    return {
        "schema_version": "fomc-pit-backfill.v1",
        "status": "PASS",
        "source": "Board of Governors of the Federal Reserve System",
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "response_count": len(responses),
        "statement_count": len(events),
        "feature_observation_count": len(features),
        "feature_names": [FOMC_FEATURE_NAME],
        "responses": [asdict(item) for item in responses],
        "pit_policy": (
            "available_at is the explicit release time printed on each official "
            "Federal Reserve FOMC statement page; state resets after 24 hours"
        ),
        "calendar_date_substituted_for_release_time": False,
        "guessed_release_time": False,
    }


__all__: Sequence[str] = (
    "FOMC_FEATURE_NAME",
    "FOMC_FEATURE_SOURCE",
    "FOMCPITStore",
    "HTTPPayload",
    "backfill_fomc_pit",
)
