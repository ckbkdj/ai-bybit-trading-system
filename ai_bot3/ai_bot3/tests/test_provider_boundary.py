from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.base import ProviderStatus
from core.providers.json_provider import JsonProvider, JsonProviderConfig


class ProviderBoundaryTests(unittest.TestCase):
    def test_internal_json_transport_is_injected_and_cutoff_is_explicit(self):
        calls = []

        def transport(url, **kwargs):
            calls.append((url, kwargs))
            return {"value": 3.1}

        provider = JsonProvider(
            JsonProviderConfig(
                "internal-macro", "https://internal.example/macro", "A",
                headers={"Authorization": "Bearer injected-at-runtime"},
            ),
            transport,
            lambda payload, cutoff, config: [{"value": payload["value"], "available_at": cutoff}],
        )
        cutoff = datetime(2026, 8, 22, tzinfo=timezone.utc)
        result = provider.fetch(as_of=cutoff)
        self.assertEqual(result.status, ProviderStatus.OK)
        self.assertEqual(result.data[0]["available_at"], cutoff)
        self.assertEqual(calls[0][1]["params"]["as_of"], "2026-08-22T00:00:00Z")

    def test_transport_failure_becomes_outage_without_leaking_headers(self):
        def broken(*args, **kwargs):
            raise TimeoutError("internal feed timed out")

        config = JsonProviderConfig(
            "internal-feed", "https://internal.example/feed", "B",
            headers={"X-Api-Key": "do-not-log"},
        )
        result = JsonProvider(config, broken, lambda *args: []).fetch(
            as_of=datetime.now(timezone.utc)
        )
        self.assertEqual(result.status, ProviderStatus.OUTAGE)
        self.assertNotIn("do-not-log", result.error)


if __name__ == "__main__":
    unittest.main()
