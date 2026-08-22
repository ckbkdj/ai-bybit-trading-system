from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone


def event_fingerprint(event_type: str, title: str, event_time: datetime, entity_ids: list[str]) -> str:
    normalized_title = re.sub(r"\W+", " ", title.casefold()).strip()
    bucket = event_time.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()
    entities = ",".join(sorted({item.strip().upper() for item in entity_ids if item.strip()}))
    digest = hashlib.sha256(f"{event_type.casefold()}|{normalized_title}|{bucket}|{entities}".encode()).hexdigest()
    return f"evt_{digest[:32]}"
