from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.features.point_in_time_store import FeatureObservation, PointInTimeFeatureStore
from core.features.quality import DataQualityScore
from core.features.registry import FactorDefinition, FactorRegistry, default_registry
from core.features.state_graph import StateInput, aggregate_state


UTC = timezone.utc


class PointInTimeFeatureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.registry = FactorRegistry(
            [
                FactorDefinition(
                    "macro_cpi_yoy", "macro.us_growth_inflation.v1", "percent",
                    "direct_observation", 172800, 0.8,
                ),
                FactorDefinition(
                    "us_risk_appetite_score", "crossasset.risk_appetite.v1", "score",
                    "inferred_rotation_proxy", 7200, 0.7,
                ),
            ]
        )
        self.store = PointInTimeFeatureStore(Path(self.temp.name) / "features.sqlite3", self.registry)

    def tearDown(self):
        self.temp.cleanup()

    def observation(
        self,
        observation_id,
        *,
        name="macro_cpi_yoy",
        value=3.1,
        event_time,
        published_at,
        available_at,
        ingested_at,
        revision_id="original",
        unit="percent",
    ):
        return FeatureObservation(
            observation_id=observation_id,
            name=name,
            value=value,
            unit=unit,
            event_time=event_time,
            published_at=published_at,
            available_at=available_at,
            ingested_at=ingested_at,
            source="official",
            source_tier="A",
            revision_id=revision_id,
            quality=0.95,
        )

    def test_available_at_blocks_future_leakage(self):
        event = datetime(2026, 8, 1, 0, tzinfo=UTC)
        published = datetime(2026, 8, 21, 12, 30, tzinfo=UTC)
        self.store.append(
            self.observation(
                "obs_future_001",
                event_time=event,
                published_at=published,
                available_at=published + timedelta(seconds=10),
                ingested_at=published + timedelta(seconds=12),
            )
        )
        before = self.store.snapshot(["macro_cpi_yoy"], published + timedelta(seconds=5))
        after = self.store.snapshot(["macro_cpi_yoy"], published + timedelta(seconds=20))
        self.assertIsNone(before.values["macro_cpi_yoy"].value)
        self.assertEqual(after.values["macro_cpi_yoy"].value, 3.1)

    def test_macro_revision_cannot_backfill_old_snapshot(self):
        event = datetime(2026, 7, 1, tzinfo=UTC)
        original_available = datetime(2026, 8, 1, 12, 30, tzinfo=UTC)
        revision_available = datetime(2026, 8, 15, 12, 30, tzinfo=UTC)
        self.store.append(
            self.observation(
                "obs_cpi_original",
                value=3.1,
                event_time=event,
                published_at=original_available,
                available_at=original_available,
                ingested_at=original_available + timedelta(seconds=1),
            )
        )
        self.store.append(
            self.observation(
                "obs_cpi_revision",
                value=3.2,
                event_time=event,
                published_at=revision_available,
                available_at=revision_available,
                ingested_at=revision_available + timedelta(seconds=1),
                revision_id="revision-1",
            )
        )
        historical = self.store.snapshot(["macro_cpi_yoy"], datetime(2026, 8, 2, tzinfo=UTC))
        revised = self.store.snapshot(["macro_cpi_yoy"], datetime(2026, 8, 16, tzinfo=UTC))
        self.assertEqual(historical.values["macro_cpi_yoy"].value, 3.1)
        self.assertEqual(historical.values["macro_cpi_yoy"].revision_id, "original")
        self.assertEqual(revised.values["macro_cpi_yoy"].value, 3.2)
        self.assertEqual(revised.values["macro_cpi_yoy"].revision_id, "revision-1")

    def test_source_outage_blocks_snapshot(self):
        at = datetime(2026, 8, 21, 8, tzinfo=UTC)
        self.store.source_event("official", "outage", "timeout", at)
        snapshot = self.store.snapshot(["macro_cpi_yoy"], at + timedelta(minutes=1))
        self.assertEqual(snapshot.status, "blocked")
        self.assertTrue(any(warning.startswith("source_outage:official") for warning in snapshot.warnings))

    def test_rotation_proxy_is_never_mislabeled_as_direct_flow(self):
        definition = self.registry.require("us_risk_appetite_score")
        self.assertEqual(definition.semantics, "inferred_rotation_proxy")
        self.assertNotEqual(definition.semantics, "directly_observed_flow")

    def test_default_registry_has_formal_factor_groups(self):
        registry = default_registry()
        self.assertEqual(
            registry.require("stablecoin_exchange_netflow_1h").semantics,
            "directly_observed_flow",
        )
        self.assertEqual(
            registry.require("gold_rotation_score").semantics,
            "inferred_rotation_proxy",
        )

    def test_state_graph_applies_reliability_freshness_and_explicit_semantics(self):
        score = aggregate_state(
            "usd_liquidity_score",
            {
                "fed_balance": StateInput(0.8, 1.0, 60, 3600, 1.0, "direct_observation"),
                "gold_proxy": StateInput(-0.4, 0.5, 7200, 3600, -0.5, "inferred_rotation_proxy"),
            },
            expected_factor_count=4,
        )
        self.assertGreater(score.value, 0)
        self.assertEqual(score.coverage, 0.5)
        self.assertEqual(score.directly_observed_count, 1)
        self.assertEqual(score.inferred_count, 1)

    def test_quality_outage_is_fail_closed(self):
        healthy = DataQualityScore(0.95, 0.95, 0.95, 0.95)
        outage = DataQualityScore(1, 1, 1, 1, source_outage=True)
        self.assertTrue(healthy.permits_ticket(0.9))
        self.assertFalse(outage.permits_ticket(0.1))


if __name__ == "__main__":
    unittest.main()
