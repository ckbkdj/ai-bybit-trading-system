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
class FlowFeatureContract:
    unit: str
    sources: tuple[str, ...]
    maximum_age_sec: int


FLOW_FEATURE_CONTRACTS: Mapping[str, FlowFeatureContract] = {
    "stablecoin_net_issuance_1d_usd": FlowFeatureContract(
        "usd", ("coinmetrics.community.ledger_reconstruction",), 4 * 86_400
    ),
    "stablecoin_net_issuance_7d_usd": FlowFeatureContract(
        "usd", ("coinmetrics.community.ledger_reconstruction",), 10 * 86_400
    ),
    "stablecoin_supply_change_7d_ratio": FlowFeatureContract(
        "ratio", ("coinmetrics.community.ledger_reconstruction",), 10 * 86_400
    ),
    "digital_asset_fund_flow_weekly_usd": FlowFeatureContract(
        "usd", ("coinshares.official.weekly_publication",), 15 * 86_400
    ),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class FlowPITFeatureSource:
    """Read a frozen stablecoin issuance snapshot using strict as-of joins."""

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
                     WHERE type='table' AND name='flow_pit_observations'"""
            ).fetchone()
            if not table:
                raise RuntimeError("flow PIT database has no observation table")
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM flow_pit_observations"
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
            "response_id",
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

    def _response_evidence(
        self, response_ids: Sequence[str]
    ) -> list[dict[str, object]]:
        if not response_ids:
            return []
        placeholders = ",".join("?" for _ in response_ids)
        evidence: list[dict[str, object]] = []
        with closing(self._connect()) as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "coinmetrics_responses" in tables:
                rows = connection.execute(
                    f"""SELECT response_id,request_descriptor,requested_at,
                               received_at,http_status,content_length,
                               content_sha256,raw_response_path,
                               'coinmetrics.community' AS evidence_source
                          FROM coinmetrics_responses
                         WHERE response_id IN ({placeholders})""",
                    tuple(response_ids),
                ).fetchall()
                evidence.extend(dict(row) for row in rows)
            if "coinshares_responses" in tables:
                rows = connection.execute(
                    f"""SELECT response_id,request_descriptor,requested_at,
                               received_at,http_status,content_length,
                               content_sha256,raw_response_path,
                               'coinshares.official' AS evidence_source
                          FROM coinshares_responses
                         WHERE response_id IN ({placeholders})""",
                    tuple(response_ids),
                ).fetchall()
                evidence.extend(dict(row) for row in rows)
        evidence.sort(key=lambda item: str(item["response_id"]))
        if len(evidence) != len(set(response_ids)):
            raise RuntimeError("flow PIT response evidence is incomplete")
        for item in evidence:
            descriptor = json.loads(str(item["request_descriptor"]))
            if "api_key" in descriptor:
                raise RuntimeError("flow response evidence contains an API key")
            if int(item["http_status"]) != 200:
                raise RuntimeError("flow response evidence contains a failed request")
            digest = str(item["content_sha256"])
            if len(digest) != 64:
                raise RuntimeError("flow response evidence has an invalid content hash")
            raw_path = Path(str(item["raw_response_path"]))
            if not raw_path.is_file():
                raise RuntimeError("flow raw response evidence is missing")
            if raw_path.stat().st_size != int(item["content_length"]):
                raise RuntimeError("flow raw response length does not match evidence")
            if self.verify_raw_hashes and _sha256(raw_path) != digest:
                raise RuntimeError("flow raw response hash does not match evidence")
        return evidence

    def load(
        self,
        names: Sequence[str],
        *,
        maximum_sequence: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        requested = tuple(dict.fromkeys(str(name) for name in names))
        if not requested:
            raise ValueError("at least one flow PIT feature is required")
        unknown = sorted(set(requested).difference(FLOW_FEATURE_CONTRACTS))
        if unknown:
            raise ValueError(f"flow PIT features have no source contract: {unknown}")
        frozen_sequence = (
            self.maximum_sequence()
            if maximum_sequence is None
            else int(maximum_sequence)
        )
        if frozen_sequence < 0:
            raise ValueError("flow PIT maximum_sequence cannot be negative")
        placeholders = ",".join("?" for _ in requested)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""SELECT o.sequence,o.observation_id,o.name,o.value,o.unit,
                            o.event_time,o.available_at,o.ingested_at,o.source,
                            o.series_id,o.observation_date,o.response_id
                       FROM flow_pit_observations o
                       LEFT JOIN flow_pit_observation_invalidations i
                         ON i.observation_id=o.observation_id
                      WHERE o.name IN ({placeholders}) AND o.sequence<=?
                        AND i.observation_id IS NULL
                      ORDER BY name,available_at,sequence""",
                requested + (frozen_sequence,),
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            return frame, {
                "source": "coinmetrics.community.stablecoin_pit",
                "database": str(self.path),
                "requested_features": list(requested),
                "observation_count": 0,
                "feature_coverage": {},
                "snapshot_maximum_sequence": frozen_sequence,
                "snapshot_sha256": hashlib.sha256(b"").hexdigest(),
                "response_count": 0,
                "raw_response_hashes_verified": self.verify_raw_hashes,
            }
        # Parser corrections are append-only. For the same public release
        # timestamp, the highest active sequence is the current audited value;
        # invalidated earlier parser outputs remain in SQLite but cannot join.
        frame = (
            frame.sort_values("sequence")
            .drop_duplicates(["name", "available_at"], keep="last")
            .reset_index(drop=True)
        )
        response_ids = sorted(set(frame["response_id"].astype(str)))
        responses = self._response_evidence(response_ids)
        for column in ("event_time", "available_at", "ingested_at"):
            frame[column] = pd.to_datetime(
                frame[column], utc=True, errors="coerce", format="mixed"
            )
        if frame[["event_time", "available_at", "ingested_at"]].isna().any().any():
            raise RuntimeError("flow PIT feature timestamps are invalid")
        valid_chronology = (
            (frame["event_time"] <= frame["available_at"])
            & (frame["available_at"] <= frame["ingested_at"])
        )
        if not valid_chronology.all():
            raise RuntimeError("flow PIT feature chronology invariant failed")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        if frame["value"].isna().any():
            raise RuntimeError("flow PIT feature contains non-numeric values")
        for name, group in frame.groupby("name", sort=True):
            contract = FLOW_FEATURE_CONTRACTS[str(name)]
            if set(group["unit"].astype(str)) != {contract.unit}:
                raise RuntimeError(f"flow PIT unit contract failed for {name}")
            if not set(group["source"].astype(str)).issubset(contract.sources):
                raise RuntimeError(f"flow PIT source contract failed for {name}")
        coverage: dict[str, object] = {}
        for name, group in frame.groupby("name", sort=True):
            start = group["available_at"].min()
            end = group["available_at"].max()
            coverage[str(name)] = {
                "observations": len(group),
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "coverage_days": float((end - start).total_seconds() / 86_400.0),
                "maximum_age_sec": FLOW_FEATURE_CONTRACTS[
                    str(name)
                ].maximum_age_sec,
            }
        return frame, {
            "source": "coinmetrics.community.stablecoin_pit",
            "database": str(self.path),
            "requested_features": list(requested),
            "observation_count": len(frame),
            "feature_coverage": coverage,
            "snapshot_maximum_sequence": frozen_sequence,
            "snapshot_sha256": self._snapshot_digest(frame),
            "response_count": len(responses),
            "response_content_sha256": sorted(
                str(item["content_sha256"]) for item in responses
            ),
            "raw_response_hashes_verified": self.verify_raw_hashes,
            "pit_policy": (
                "USDC+USDT ledger-derived supply, available 48h after metric day; "
                "latest available_at at or before decision_at with staleness cutoff"
            ),
            "semantic_scope": "net issuance/redemption, not exchange netflow",
        }

    def join(
        self,
        decisions: pd.DataFrame,
        *,
        names: Sequence[str],
        history: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        if "decision_at" not in decisions:
            raise ValueError("decision_at is required for flow PIT join")
        observations = history if history is not None else self.load(names)[0]
        output = decisions.copy().reset_index(drop=False).rename(
            columns={"index": "_row_id"}
        )
        output["decision_at"] = pd.to_datetime(
            output["decision_at"], utc=True, errors="coerce"
        )
        if output["decision_at"].isna().any():
            raise ValueError("flow PIT decisions contain invalid timestamps")
        for name in names:
            contract = FLOW_FEATURE_CONTRACTS[str(name)]
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
    "FLOW_FEATURE_CONTRACTS",
    "FlowFeatureContract",
    "FlowPITFeatureSource",
)
