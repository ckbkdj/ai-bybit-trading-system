from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timedelta, timezone
from typing import Sequence

from core.providers.bybit_historical_archive import (
    orderbook_archive_url,
    trade_archive_url,
)
from core.providers.bybit_public_pit_store import BybitPublicPITStore


ARCHIVE_MARKET = "linear"
DATA_KINDS = ("orderbook", "trades")
FEATURES_BY_KIND = {
    "orderbook": {
        "orderbook_spread_bps",
        "bybit_orderbook_delta_l5",
        "orderbook_imbalance_l5",
        "ofi_1m",
        "orderbook_depth_usdt_l5",
        "microprice_deviation_bps",
        "fill_probability",
        "expected_slippage_bps",
    },
    "trades": {
        "public_trade_imbalance_1m",
        "aggressive_cvd_1m",
    },
}
SOURCE_BY_KIND = {
    "orderbook": "bybit.public.orderbook",
    "trades": "bybit.public.trades",
}
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _archive_id(
    data_kind: str, symbol: str, trading_date: date, source_url: str
) -> str:
    token = hashlib.sha256(
        (
            f"{data_kind}|{ARCHIVE_MARKET}|{symbol}|"
            f"{trading_date.isoformat()}|{source_url}"
        ).encode()
    ).hexdigest()[:48]
    return f"ba_{token}"


def _expected_url(data_kind: str, symbol: str, trading_date: date) -> str:
    if data_kind == "orderbook":
        return orderbook_archive_url(symbol, trading_date)
    if data_kind == "trades":
        return trade_archive_url(symbol, trading_date)
    raise ValueError("unsupported Bybit archive data kind")


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("archive evidence timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def _date_ranges(values: Sequence[date]) -> list[dict[str, object]]:
    if not values:
        return []
    ordered = sorted(set(values))
    output: list[dict[str, object]] = []
    first = previous = ordered[0]
    for current in ordered[1:]:
        if current == previous + timedelta(days=1):
            previous = current
            continue
        output.append(
            {
                "start": first.isoformat(),
                "end": previous.isoformat(),
                "days": (previous - first).days + 1,
            }
        )
        first = previous = current
    output.append(
        {
            "start": first.isoformat(),
            "end": previous.isoformat(),
            "days": (previous - first).days + 1,
        }
    )
    return output


def audit_historical_archive_window(
    store: BybitPublicPITStore,
    *,
    start: date,
    end: date,
    symbols: Sequence[str],
    data_kinds: Sequence[str] = DATA_KINDS,
) -> dict[str, object]:
    """Verify exact daily archive coverage and feature-to-receipt linkage."""

    if start > end:
        raise ValueError("historical archive audit start is after end")
    normalized_symbols = tuple(
        dict.fromkeys(str(value).strip().upper() for value in symbols)
    )
    normalized_kinds = tuple(dict.fromkeys(str(value) for value in data_kinds))
    if not normalized_symbols:
        raise ValueError("historical archive audit requires symbols")
    if not normalized_kinds or any(value not in DATA_KINDS for value in normalized_kinds):
        raise ValueError("historical archive audit has unsupported data kinds")

    days = tuple(
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    )
    expected_keys = {
        (data_kind, symbol, trading_date.isoformat())
        for trading_date in days
        for symbol in normalized_symbols
        for data_kind in normalized_kinds
    }
    kind_placeholders = ",".join("?" for _ in normalized_kinds)
    symbol_placeholders = ",".join("?" for _ in normalized_symbols)
    parameters = (
        *normalized_kinds,
        *normalized_symbols,
        start.isoformat(),
        end.isoformat(),
    )
    store.flush()
    with store.connect() as connection:
        archive_rows = connection.execute(
            f"""SELECT * FROM bybit_historical_archive_files
                  WHERE data_kind IN ({kind_placeholders})
                    AND symbol IN ({symbol_placeholders})
                    AND trading_date BETWEEN ? AND ?
                  ORDER BY data_kind,symbol,trading_date""",
            parameters,
        ).fetchall()
        feature_rows = connection.execute(
            f"""SELECT f.archive_id,COUNT(*) AS observation_count,
                        COUNT(DISTINCT f.symbol) AS symbol_count,
                        MIN(f.symbol) AS feature_symbol,
                        GROUP_CONCAT(DISTINCT f.name) AS feature_names,
                        COUNT(DISTINCT f.source) AS source_count,
                        MIN(f.source) AS feature_source,
                        COUNT(DISTINCT f.provenance_kind) AS provenance_count,
                        MIN(f.provenance_kind) AS provenance_kind,
                        SUM(CASE WHEN substr(f.event_time,1,10)<>a.trading_date
                                 THEN 1 ELSE 0 END) AS outside_day_count,
                        SUM(CASE WHEN NOT (
                              f.event_time<=f.available_at
                              AND f.available_at<=f.ingested_at
                            ) THEN 1 ELSE 0 END) AS chronology_violation_count,
                        SUM(CASE WHEN f.api_batch_id IS NOT NULL
                                 THEN 1 ELSE 0 END) AS api_link_count,
                        SUM(CASE WHEN length(f.payload_sha256)<>64
                                 OR lower(f.payload_sha256) GLOB '*[^0-9a-f]*'
                                 THEN 1 ELSE 0 END) AS invalid_sha_count
                   FROM bybit_feature_observations AS f
                   JOIN bybit_historical_archive_files AS a
                     ON a.archive_id=f.archive_id
                  WHERE a.data_kind IN ({kind_placeholders})
                    AND a.symbol IN ({symbol_placeholders})
                    AND a.trading_date BETWEEN ? AND ?
                  GROUP BY f.archive_id""",
            parameters,
        ).fetchall()

    archives = {
        (str(row["data_kind"]), str(row["symbol"]), str(row["trading_date"])): dict(row)
        for row in archive_rows
    }
    features = {str(row["archive_id"]): dict(row) for row in feature_rows}
    violations: list[dict[str, str]] = []

    def fail(archive_id: str, reason: str) -> None:
        violations.append({"archive_id": archive_id, "reason": reason})

    for key, archive in archives.items():
        data_kind, symbol, trading_day = key
        archive_id = str(archive["archive_id"])
        try:
            parsed_day = date.fromisoformat(trading_day)
        except ValueError:
            fail(archive_id, "invalid_trading_date")
            continue
        if str(archive["status"]) != "completed":
            continue
        expected_url = _expected_url(data_kind, symbol, parsed_day)
        if str(archive["market"]) != ARCHIVE_MARKET:
            fail(archive_id, "market_mismatch")
        if str(archive["source_url"]) != expected_url:
            fail(archive_id, "source_url_mismatch")
        if archive_id != _archive_id(data_kind, symbol, parsed_day, expected_url):
            fail(archive_id, "archive_id_mismatch")
        if (
            int(archive["content_length"]) <= 0
            or not SHA256_PATTERN.fullmatch(str(archive["content_sha256"]))
            or int(archive["rows_read"]) <= 0
            or int(archive["feature_observation_count"]) <= 0
        ):
            fail(archive_id, "archive_manifest_counts_or_hash_invalid")
        if data_kind == "orderbook" and (
            not str(archive.get("member_name") or "")
            or int(archive.get("member_size") or 0) <= 0
        ):
            fail(archive_id, "orderbook_member_manifest_missing")
        try:
            first_event = _parse_utc(archive["first_event_time"])
            last_event = _parse_utc(archive["last_event_time"])
            day_start = datetime.combine(parsed_day, datetime.min.time(), tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1)
            overlap = timedelta(seconds=10) if data_kind == "orderbook" else timedelta(0)
            ends_outside = (
                last_event > day_end + overlap
                if data_kind == "orderbook"
                else last_event >= day_end
            )
            if (
                first_event > last_event
                or first_event < day_start - overlap
                or ends_outside
            ):
                fail(archive_id, "archive_event_window_invalid")
        except (TypeError, ValueError):
            fail(archive_id, "archive_event_timestamp_invalid")

        feature = features.get(archive_id)
        feature_names = (
            set(str(feature["feature_names"]).split(",")) if feature else set()
        )
        if feature is None or (
            int(feature["observation_count"])
            != int(archive["feature_observation_count"])
            or int(feature["symbol_count"]) != 1
            or str(feature["feature_symbol"]) != symbol
            or feature_names != FEATURES_BY_KIND[data_kind]
            or int(feature["source_count"]) != 1
            or str(feature["feature_source"]) != SOURCE_BY_KIND[data_kind]
            or int(feature["provenance_count"]) != 1
            or str(feature["provenance_kind"]) != "historical_archive_replay"
            or int(feature["outside_day_count"]) != 0
            or int(feature["chronology_violation_count"]) != 0
            or int(feature["api_link_count"]) != 0
            or int(feature["invalid_sha_count"]) != 0
        ):
            fail(archive_id, "feature_linkage_mismatch")

    completed_keys = {
        key for key, row in archives.items() if str(row["status"]) == "completed"
    }
    failed_keys = {
        key for key, row in archives.items() if str(row["status"]) == "failed"
    }
    series: list[dict[str, object]] = []
    for symbol in normalized_symbols:
        for data_kind in normalized_kinds:
            expected_series = {
                (data_kind, symbol, trading_date.isoformat()) for trading_date in days
            }
            completed_dates = sorted(
                date.fromisoformat(key[2])
                for key in expected_series.intersection(completed_keys)
            )
            failed_dates = sorted(
                date.fromisoformat(key[2])
                for key in expected_series.intersection(failed_keys)
            )
            missing_dates = sorted(
                date.fromisoformat(key[2])
                for key in expected_series.difference(set(archives))
            )
            series.append(
                {
                    "symbol": symbol,
                    "data_kind": data_kind,
                    "expected_days": len(days),
                    "completed_days": len(completed_dates),
                    "failed_days": len(failed_dates),
                    "missing_days": len(missing_dates),
                    "first_completed_date": (
                        completed_dates[0].isoformat() if completed_dates else None
                    ),
                    "last_completed_date": (
                        completed_dates[-1].isoformat() if completed_dates else None
                    ),
                    "failed_date_ranges": _date_ranges(failed_dates),
                    "missing_date_ranges": _date_ranges(missing_dates),
                }
            )

    completed_count = len(expected_keys.intersection(completed_keys))
    failed_count = len(expected_keys.intersection(failed_keys))
    missing_count = len(expected_keys.difference(set(archives)))
    complete = (
        completed_count == len(expected_keys)
        and failed_count == 0
        and missing_count == 0
        and not violations
    )
    return {
        "schema_version": "bybit-historical-archive-audit.v1",
        "status": "VERIFIED_COMPLETE" if complete else "FAILED",
        "complete": complete,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "symbols": list(normalized_symbols),
        "data_kinds": list(normalized_kinds),
        "expected_files": len(expected_keys),
        "completed_files": completed_count,
        "failed_files": failed_count,
        "missing_files": missing_count,
        "integrity_violation_count": len(violations),
        "integrity_violations": violations,
        "series": series,
        "raw_archive_retention": (
            "The compressed source file is only retained when --keep-archives is used; "
            "this audit verifies the immutable download SHA receipt and derived PIT linkage."
        ),
    }


__all__ = ("audit_historical_archive_window",)
