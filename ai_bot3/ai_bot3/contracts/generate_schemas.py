from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .execution_receipt_v1 import ExecutionReceipt
from .forecast_v1 import ForecastEnvelope
from .operation_ticket_v1 import OperationTicket


SCHEMAS = {
    "forecast-envelope.v1.json": (ForecastEnvelope, "forecast-envelope.v1"),
    "operation-ticket.v1.json": (OperationTicket, "operation-ticket.v1"),
    "execution-receipt.v1.json": (ExecutionReceipt, "execution-receipt.v1"),
}


def generate(output_dir: Path | None = None) -> list[Path]:
    destination = output_dir or Path(__file__).resolve().parent / "schemas"
    destination.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for filename, (model, schema_id) in SCHEMAS.items():
        schema = model.model_json_schema(mode="validation")
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["$id"] = schema_id
        target = destination / filename
        descriptor, temporary = tempfile.mkstemp(prefix=f".{filename}.", dir=str(destination))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(schema, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        written.append(target)
    return written


if __name__ == "__main__":
    for path in generate():
        print(path)
