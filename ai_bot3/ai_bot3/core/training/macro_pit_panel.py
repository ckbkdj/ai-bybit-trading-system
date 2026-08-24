from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd


@dataclass(frozen=True)
class MacroFeatureContract:
    unit: str
    sources: tuple[str, ...]
    maximum_age_sec: int


MACRO_FEATURE_CONTRACTS: Mapping[str, MacroFeatureContract] = {
    "vix_level": MacroFeatureContract(
        "index_points", ("fred.alfred.initial_release",), 5 * 86_400
    ),
    "real_yield_10y": MacroFeatureContract(
        "percent", ("fred.alfred.initial_release",), 5 * 86_400
    ),
    "fred_cpi_first_release_yoy_ratio": MacroFeatureContract(
        "ratio", ("fred.alfred.initial_release",), 45 * 86_400
    ),
    "fred_payrolls_first_release_change_thousands": MacroFeatureContract(
        "thousands_of_persons", ("fred.alfred.initial_release",), 45 * 86_400
    ),
    "fred_unemployment_first_release_pct": MacroFeatureContract(
        "percent", ("fred.alfred.initial_release",), 45 * 86_400
    ),
    "alfred_cpi_mean_revision_delta": MacroFeatureContract(
        "source_units", ("fred.alfred.revision_history",), 400 * 86_400
    ),
    "alfred_payrolls_mean_revision_delta": MacroFeatureContract(
        "source_units", ("fred.alfred.revision_history",), 45 * 86_400
    ),
    "tier_a_event_state": MacroFeatureContract(
        "binary_24h_post_release_window",
        ("fred.alfred.release_vintage",),
        45 * 86_400,
    ),
    "fomc_statement_event_state": MacroFeatureContract(
        "binary_24h_post_release_window",
        ("federal_reserve.fomc_statement",),
        70 * 86_400,
    ),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class MacroPITFeatureSource:
    """Read frozen official macro observations using strict as-of joins."""

    def __init__(self, path: Path, *, verify_raw_hashes: bool = True) -> None:
        self.path = Path(path).expanduser().resolve()
        self.verify_raw_hashes = bool(verify_raw_hashes)
        if not self.path.is_file():
            raise FileNotFoundError(self.path)

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def maximum_sequence(self) -> int:
        with closing(self._connect()) as connection:
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='macro_pit_observations'"""
            ).fetchone()
            if not table:
                raise RuntimeError("macro PIT database has no observation table")
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM macro_pit_observations"
            ).fetchone()
        return int(row[0])

    @staticmethod
    def _snapshot_digest(frame: pd.DataFrame) -> str:
        columns = [
            "sequence",
            "observation_id",
            "name",
            "value",
            "unit",
            "event_time",
            "available_at",
            "ingested_at",
            "source",
            "series_id",
            "observation_date",
            "vintage_date",
        ]
        payload = frame[columns].copy().sort_values("sequence")
        for column in ("event_time", "available_at", "ingested_at"):
            payload[column] = pd.to_datetime(payload[column], utc=True).astype(str)
        digest = hashlib.sha256()
        digest.update(json.dumps(columns, separators=(",", ":")).encode())
        digest.update(
            pd.util.hash_pandas_object(payload, index=False, categorize=True)
            .to_numpy(dtype="uint64")
            .tobytes()
        )
        return digest.hexdigest()

    def _fred_response_evidence(
        self, *, received_at_maximum: str
    ) -> list[dict[str, object]]:
        with closing(self._connect()) as connection:
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='fred_alfred_responses'"""
            ).fetchone()
            if not table:
                raise RuntimeError("macro PIT database has no response evidence")
            rows = connection.execute(
                """SELECT response_id,series_id,output_type,request_descriptor,
                          requested_at,received_at,http_status,content_length,
                          content_sha256,row_count,raw_response_path
                     FROM fred_alfred_responses
                    WHERE received_at<=?
                    ORDER BY series_id,output_type,response_id""",
                (received_at_maximum,),
            ).fetchall()
        evidence = [dict(row) for row in rows]
        if not evidence:
            raise RuntimeError("macro PIT database has no official API responses")
        for item in evidence:
            descriptor = json.loads(str(item["request_descriptor"]))
            if "api_key" in descriptor:
                raise RuntimeError("macro response evidence contains an API key")
            if int(item["http_status"]) != 200:
                raise RuntimeError("macro response evidence contains a failed request")
            digest = str(item["content_sha256"])
            if len(digest) != 64:
                raise RuntimeError("macro response evidence has an invalid content hash")
            raw_path = Path(str(item["raw_response_path"]))
            if not raw_path.is_file():
                raise RuntimeError("macro raw response evidence is missing")
            if raw_path.stat().st_size != int(item["content_length"]):
                raise RuntimeError("macro raw response length does not match evidence")
            if self.verify_raw_hashes and _sha256(raw_path) != digest:
                raise RuntimeError("macro raw response hash does not match evidence")
        for item in evidence:
            item["source_system"] = "fred_alfred"
        return evidence

    def _official_response_evidence(
        self,
        *,
        received_at_maximum: str,
        required_sources: set[str],
    ) -> list[dict[str, object]]:
        with closing(self._connect()) as connection:
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='official_macro_responses'"""
            ).fetchone()
            if not table:
                raise RuntimeError("macro PIT database has no official response evidence")
            placeholders = ",".join("?" for _ in required_sources)
            rows = connection.execute(
                f"""SELECT response_id,source,document_kind,request_url,
                            requested_at,received_at,http_status,content_length,
                            content_sha256,row_count,raw_response_path
                       FROM official_macro_responses
                      WHERE source IN ({placeholders}) AND received_at<=?
                      ORDER BY source,document_kind,response_id""",
                tuple(sorted(required_sources)) + (received_at_maximum,),
            ).fetchall()
        evidence = [dict(row) for row in rows]
        if not evidence:
            raise RuntimeError("macro PIT database has no matching official responses")
        for item in evidence:
            if int(item["http_status"]) != 200:
                raise RuntimeError("official macro evidence contains a failed request")
            url = str(item["request_url"])
            if not url.startswith("https://www.federalreserve.gov/"):
                raise RuntimeError("official macro evidence has an unapproved origin")
            digest = str(item["content_sha256"])
            if len(digest) != 64:
                raise RuntimeError("official macro evidence has an invalid content hash")
            raw_path = Path(str(item["raw_response_path"]))
            if not raw_path.is_file():
                raise RuntimeError("official macro raw response evidence is missing")
            if raw_path.stat().st_size != int(item["content_length"]):
                raise RuntimeError("official macro raw response length does not match evidence")
            if self.verify_raw_hashes and _sha256(raw_path) != digest:
                raise RuntimeError("official macro raw response hash does not match evidence")
            item["source_system"] = "official_macro"
        return evidence

    def _response_evidence(
        self,
        *,
        received_at_maximum: str,
        observed_sources: set[str],
    ) -> list[dict[str, object]]:
        evidence: list[dict[str, object]] = []
        if any(source.startswith("fred.alfred.") for source in observed_sources):
            evidence.extend(
                self._fred_response_evidence(received_at_maximum=received_at_maximum)
            )
        official_sources = {
            source
            for source in observed_sources
            if source.startswith("federal_reserve.")
        }
        if official_sources:
            evidence.extend(
                self._official_response_evidence(
                    received_at_maximum=received_at_maximum,
                    required_sources=official_sources,
                )
            )
        if not evidence:
            raise RuntimeError("macro PIT observations have no response evidence contract")
        return evidence

    def load(
        self,
        names: Sequence[str],
        *,
        maximum_sequence: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        requested = tuple(dict.fromkeys(str(name) for name in names))
        if not requested:
            raise ValueError("at least one macro PIT feature is required")
        unknown = sorted(set(requested).difference(MACRO_FEATURE_CONTRACTS))
        if unknown:
            raise ValueError(f"macro PIT features have no source contract: {unknown}")
        frozen_sequence = (
            self.maximum_sequence()
            if maximum_sequence is None
            else int(maximum_sequence)
        )
        if frozen_sequence < 0:
            raise ValueError("macro PIT maximum_sequence cannot be negative")
        placeholders = ",".join("?" for _ in requested)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""SELECT sequence,observation_id,name,value,unit,event_time,
                            available_at,ingested_at,source,series_id,
                            observation_date,vintage_date
                       FROM macro_pit_observations
                      WHERE name IN ({placeholders}) AND sequence<=?
                      ORDER BY name,available_at,sequence""",
                requested + (frozen_sequence,),
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            return frame, {
                "source": "fred.alfred.pit",
                "database": str(self.path),
                "requested_features": list(requested),
                "observation_count": 0,
                "feature_coverage": {},
                "snapshot_maximum_sequence": frozen_sequence,
                "snapshot_sha256": hashlib.sha256(b"").hexdigest(),
                "response_count": 0,
                "raw_response_hashes_verified": self.verify_raw_hashes,
            }
        received_at_maximum = str(frame["ingested_at"].max())
        observed_sources = set(frame["source"].astype(str))
        for column in ("event_time", "available_at", "ingested_at"):
            frame[column] = pd.to_datetime(
                frame[column], utc=True, errors="coerce", format="mixed"
            )
        if frame[["event_time", "available_at", "ingested_at"]].isna().any().any():
            raise RuntimeError("macro PIT feature timestamps are invalid")
        chronology = ~(
            (frame["event_time"] <= frame["available_at"])
            & (frame["available_at"] <= frame["ingested_at"])
        )
        if chronology.any():
            raise RuntimeError("macro PIT feature chronology invariant failed")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        if frame["value"].isna().any():
            raise RuntimeError("macro PIT feature contains non-numeric values")
        responses = self._response_evidence(
            received_at_maximum=received_at_maximum,
            observed_sources=observed_sources,
        )
        for name, group in frame.groupby("name", sort=True):
            contract = MACRO_FEATURE_CONTRACTS[str(name)]
            if set(group["unit"].astype(str)) != {contract.unit}:
                raise RuntimeError(f"macro PIT unit contract failed for {name}")
            if not set(group["source"].astype(str)).issubset(contract.sources):
                raise RuntimeError(f"macro PIT source contract failed for {name}")
        coverage: dict[str, object] = {}
        for name, group in frame.groupby("name", sort=True):
            start = group["available_at"].min()
            end = group["available_at"].max()
            coverage[str(name)] = {
                "observations": len(group),
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "coverage_days": float((end - start).total_seconds() / 86_400.0),
                "maximum_age_sec": MACRO_FEATURE_CONTRACTS[
                    str(name)
                ].maximum_age_sec,
            }
        return frame, {
            "source": "fred.alfred.pit",
            "database": str(self.path),
            "requested_features": list(requested),
            "observation_count": len(frame),
            "feature_coverage": coverage,
            "feature_source_contracts": {
                name: list(MACRO_FEATURE_CONTRACTS[name].sources)
                for name in requested
            },
            "snapshot_maximum_sequence": frozen_sequence,
            "snapshot_sha256": self._snapshot_digest(frame),
            "response_count": len(responses),
            "response_content_sha256": sorted(
                str(item["content_sha256"]) for item in responses
            ),
            "raw_response_hashes_verified": self.verify_raw_hashes,
            "pit_policy": (
                "global latest available_at at or before decision_at with "
                "feature-specific staleness cutoff"
            ),
        }

    def join(
        self,
        decisions: pd.DataFrame,
        *,
        names: Sequence[str],
        history: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        if "decision_at" not in decisions:
            raise ValueError("decision_at is required for macro PIT join")
        observations = history if history is not None else self.load(names)[0]
        output = decisions.copy().reset_index(drop=False).rename(
            columns={"index": "_row_id"}
        )
        output["decision_at"] = pd.to_datetime(
            output["decision_at"], utc=True, errors="coerce"
        )
        if output["decision_at"].isna().any():
            raise ValueError("macro PIT decisions contain invalid timestamps")
        for name in names:
            contract = MACRO_FEATURE_CONTRACTS[str(name)]
            subset = observations[observations["name"] == name][
                ["sequence", "available_at", "value"]
            ].copy()
            available_column = f"{name}__available_at"
            if subset.empty:
                output[name] = float("nan")
                output[available_column] = pd.NaT
                continue
            subset = subset.rename(
                columns={"available_at": available_column, "value": name}
            ).sort_values([available_column, "sequence"])
            output = pd.merge_asof(
                output.sort_values("decision_at"),
                subset,
                left_on="decision_at",
                right_on=available_column,
                direction="backward",
                tolerance=pd.Timedelta(seconds=contract.maximum_age_sec),
                allow_exact_matches=True,
            ).drop(columns=["sequence"])
            violation = output[available_column].notna() & (
                output[available_column] > output["decision_at"]
            )
            if violation.any():
                raise RuntimeError(f"PIT violation while joining {name}")
        return output.sort_values("_row_id").drop(columns=["_row_id"]).reset_index(
            drop=True
        )


__all__: Sequence[str] = (
    "MACRO_FEATURE_CONTRACTS",
    "MacroFeatureContract",
    "MacroPITFeatureSource",
)
