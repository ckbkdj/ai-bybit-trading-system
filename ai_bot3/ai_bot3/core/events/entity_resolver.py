from __future__ import annotations


class EntityResolver:
    def __init__(self, aliases: dict[str, str]):
        self.aliases = {key.casefold().strip(): value.strip().upper() for key, value in aliases.items()}

    def resolve(self, mentions: list[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
        resolved, unresolved = set(), set()
        for mention in mentions:
            normalized = mention.casefold().strip()
            entity = self.aliases.get(normalized)
            (resolved if entity else unresolved).add(entity or mention.strip())
        return tuple(sorted(resolved)), tuple(sorted(item for item in unresolved if item))
