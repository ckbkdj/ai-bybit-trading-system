from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from contracts.event_impact_v1 import EventImpactVector
from core.events.deduplicator import event_fingerprint
from core.events.impact_model import build_impact_vector
from core.events.source_ranker import EvidenceSource, SourceTier
from core.jobs.job_store import ResearchJobStore
from core.jobs.research_job import ResearchState


NOW = datetime(2026, 8, 22, 8, tzinfo=timezone.utc)


def vector(*, blackout=True, primary=True):
    return EventImpactVector.model_validate(
        {
            "event_id": "evt-fed-001",
            "revision": 1,
            "event_type": "central_bank_policy",
            "data_cutoff": NOW,
            "created_at": NOW + timedelta(hours=2),
            "novelty": 0.82,
            "source_reliability": 0.96,
            "confirmation_count": 3,
            "primary_source_verified": primary,
            "affected_assets": {
                "BTC": {
                    "direction_distribution": {
                        "positive": 0.18,
                        "neutral": 0.12,
                        "negative": 0.70,
                    },
                    "impact_strength": 0.73,
                    "half_life_sec": 21600,
                }
            },
            "scenarios": [
                {"name": "base", "probability": 0.7},
                {"name": "reversal", "probability": 0.3},
            ],
            "event_blackout": blackout,
            "blackout_until": NOW + timedelta(hours=8) if blackout else None,
            "evidence_source_ids": ["fed-statement"],
        }
    )


class ResearchJobTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "research.sqlite3"
        self.store = ResearchJobStore(self.path)
        self.store.record_event(
            "evt-fed-001", "central_bank_policy", NOW, NOW,
            {"title": "Federal Reserve statement"},
        )

    def tearDown(self):
        self.temp.cleanup()

    def advance_to_quantifying(self, job_id):
        for state in (
            ResearchState.DEDUPLICATED,
            ResearchState.TRIAGED,
            ResearchState.PRIMARY_SOURCE_VERIFYING,
            ResearchState.ENTITY_RESOLVED,
            ResearchState.SCENARIOS_BUILDING,
            ResearchState.IMPACT_QUANTIFYING,
        ):
            self.store.transition(job_id, state, {"step": state.value})

    def test_job_checkpoints_resume_and_revision_survive_restart(self):
        job_id = self.store.create_job(["evt-fed-001"], NOW)
        self.advance_to_quantifying(job_id)
        self.store.add_event_source(
            "evt-fed-001",
            source_tier="A",
            source_uri="https://official.example/statement",
            published_at=NOW,
            verified=True,
            content={"statement": "verified"},
        )
        self.assertEqual(self.store.save_revision(job_id, vector()), 1)

        # Re-open the database as a restarted worker; no in-memory state is required.
        restarted = ResearchJobStore(self.path)
        self.assertEqual(restarted.get(job_id)["status"], ResearchState.IMPACT_QUANTIFYING.value)
        self.assertGreaterEqual(len(restarted.checkpoints(job_id)), 8)
        self.assertEqual(restarted.revisions(job_id)[0].event_id, "evt-fed-001")
        restarted.complete(job_id)
        self.assertEqual(restarted.get(job_id)["status"], ResearchState.COMPLETED.value)
        self.assertTrue(restarted.event_blackout("BTC", NOW + timedelta(hours=3)))

    def test_tier_c_alone_cannot_trigger_blackout(self):
        job_id = self.store.create_job(["evt-fed-001"], NOW)
        self.advance_to_quantifying(job_id)
        self.store.add_event_source(
            "evt-fed-001",
            source_tier="C",
            source_uri="https://social.example/post",
            published_at=NOW,
            verified=False,
            content={"claim": "unverified"},
        )
        with self.assertRaisesRegex(ValueError, "Tier A"):
            self.store.save_revision(job_id, vector())

    def test_research_job_can_be_superseded_without_mutating_history(self):
        old = self.store.create_job(["evt-fed-001"], NOW)
        new = self.store.create_job(["evt-fed-001"], NOW + timedelta(hours=1))
        self.store.supersede(old, new)
        self.assertEqual(self.store.get(old)["status"], ResearchState.SUPERSEDED.value)
        self.assertEqual(self.store.get(old)["superseded_by_job_id"], new)
        self.assertEqual(self.store.get(new)["status"], ResearchState.DETECTED.value)

    def test_event_pipeline_is_deterministic_and_blackout_requires_primary_source(self):
        fingerprint = event_fingerprint(
            "central_bank_policy", "Fed policy statement", NOW, ["FED", "BTC"]
        )
        self.assertEqual(
            fingerprint,
            event_fingerprint("central_bank_policy", "Fed policy statement", NOW, ["BTC", "FED"]),
        )
        tier_c = [EvidenceSource("social-1", SourceTier.C, NOW, False, 0.4)]
        with self.assertRaisesRegex(ValueError, "Tier A"):
            build_impact_vector(
                event_id=fingerprint,
                revision=1,
                event_type="central_bank_policy",
                data_cutoff=NOW,
                created_at=NOW + timedelta(minutes=5),
                novelty=0.8,
                sources=tier_c,
                affected_assets={
                    "BTC": {
                        "direction_distribution": {"positive": 0.2, "neutral": 0.2, "negative": 0.6},
                        "impact_strength": 0.7,
                        "half_life_sec": 3600,
                    }
                },
                scenarios=[{"name": "base", "probability": 3}, {"name": "reversal", "probability": 1}],
                event_blackout=True,
                blackout_until=NOW + timedelta(hours=2),
            )


if __name__ == "__main__":
    unittest.main()
