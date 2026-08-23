from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT))

from contracts.execution_receipt_v1 import ExecutionReceipt
from contracts.operation_ticket_v1 import OperationTicket


class ContractSchemaParityTests(unittest.TestCase):
    def assert_schema_matches_prediction_source(self, filename, model):
        path = (
            WORKSPACE
            / "ai_bot3"
            / "ai_bot3"
            / "contracts"
            / "schemas"
            / filename
        )
        expected = json.loads(path.read_text(encoding="utf-8"))
        expected.pop("$schema", None)
        expected.pop("$id", None)
        self.assertEqual(model.model_json_schema(mode="validation"), expected)

    def test_operation_ticket_schema_is_identical_across_services(self):
        self.assert_schema_matches_prediction_source(
            "operation-ticket.v1.json", OperationTicket
        )

    def test_execution_receipt_schema_is_identical_across_services(self):
        self.assert_schema_matches_prediction_source(
            "execution-receipt.v1.json", ExecutionReceipt
        )


if __name__ == "__main__":
    unittest.main()
