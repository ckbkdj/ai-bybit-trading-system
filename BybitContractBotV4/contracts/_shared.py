from __future__ import annotations

import sys
from pathlib import Path


def ensure_shared_contracts_path() -> None:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "shadow_contracts" / "__init__.py").is_file():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)
            return
    raise RuntimeError("workspace shared contract package is missing")
