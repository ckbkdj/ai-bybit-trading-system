from __future__ import annotations

import ast
import io
import logging
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from logger import SecretRedactionFilter


class SecurityBoundaryTests(unittest.TestCase):
    def test_execution_code_does_not_import_prediction_internals(self):
        forbidden_prefixes = ("ai_bot3", "core.inferencer", "core.portfolio", "core.result_manager")
        violations = []
        for path in ROOT.rglob("*.py"):
            if path.name.startswith("bot_threshold_super_v") and path.name != "bot_threshold_super_v4_1.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                else:
                    continue
                for name in names:
                    if name.startswith(forbidden_prefixes):
                        violations.append(f"{path.name}:{name}")
        self.assertEqual(violations, [])

    def test_logger_redacts_credentials_and_signatures(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.addFilter(SecretRedactionFilter())
        test_logger = logging.getLogger("security-redaction-test")
        test_logger.handlers.clear()
        test_logger.propagate = False
        test_logger.setLevel(logging.INFO)
        test_logger.addHandler(handler)
        test_logger.info(
            "api_key=%s secret_key=%s Authorization: Bearer %s X-BAPI-SIGN=%s",
            "example-api-key-value",
            "example-secret-value",
            "example-bearer-value",
            "example-signature-value",
        )
        output = stream.getvalue()
        self.assertNotIn("example-api-key-value", output)
        self.assertNotIn("example-secret-value", output)
        self.assertNotIn("example-bearer-value", output)
        self.assertNotIn("example-signature-value", output)
        self.assertGreaterEqual(output.count("<redacted>"), 4)


if __name__ == "__main__":
    unittest.main()
