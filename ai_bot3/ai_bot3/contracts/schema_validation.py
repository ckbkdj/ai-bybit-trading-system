from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


class SchemaValidationError(ValueError):
    pass


def _resolve(root: Mapping[str, Any], node: Mapping[str, Any]) -> Mapping[str, Any]:
    reference = node.get("$ref")
    if not reference:
        return node
    if not str(reference).startswith("#/"):
        raise SchemaValidationError(f"external schema reference is not supported: {reference}")
    current: Any = root
    for part in str(reference)[2:].split("/"):
        current = current[part.replace("~1", "/").replace("~0", "~")]
    if not isinstance(current, Mapping):
        raise SchemaValidationError(f"schema reference does not resolve to an object: {reference}")
    return current


def _is_type(instance: Any, expected: str) -> bool:
    if expected == "null":
        return instance is None
    if expected == "object":
        return isinstance(instance, Mapping)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected == "number":
        return isinstance(instance, (int, float)) and not isinstance(instance, bool)
    return True


def validate_json_schema(instance: Any, schema: Mapping[str, Any]) -> None:
    """Validate the Pydantic-generated JSON Schema subset used by v1 contracts."""

    def visit(value: Any, node: Mapping[str, Any], path: str) -> None:
        node = _resolve(schema, node)
        alternatives = node.get("anyOf") or node.get("oneOf")
        if isinstance(alternatives, list):
            failures = []
            for alternative in alternatives:
                try:
                    visit(value, alternative, path)
                    return
                except SchemaValidationError as exc:
                    failures.append(str(exc))
            raise SchemaValidationError(f"{path}: no schema alternative matched: {'; '.join(failures)}")

        if "const" in node and value != node["const"]:
            raise SchemaValidationError(f"{path}: expected constant {node['const']!r}")
        if "enum" in node and value not in node["enum"]:
            raise SchemaValidationError(f"{path}: value is not in enum")

        expected = node.get("type")
        if isinstance(expected, list):
            if not any(_is_type(value, option) for option in expected):
                raise SchemaValidationError(f"{path}: invalid type")
        elif isinstance(expected, str) and not _is_type(value, expected):
            raise SchemaValidationError(f"{path}: expected {expected}")

        if isinstance(value, Mapping):
            required = node.get("required") or []
            missing = [key for key in required if key not in value]
            if missing:
                raise SchemaValidationError(f"{path}: missing required fields {missing}")
            properties = node.get("properties") or {}
            if node.get("additionalProperties") is False:
                extras = set(value) - set(properties)
                if extras:
                    raise SchemaValidationError(f"{path}: unknown fields {sorted(extras)}")
            for key, item in value.items():
                child = properties.get(key)
                if isinstance(child, Mapping):
                    visit(item, child, f"{path}.{key}")
                elif isinstance(node.get("additionalProperties"), Mapping):
                    visit(item, node["additionalProperties"], f"{path}.{key}")
        elif isinstance(value, list) and isinstance(node.get("items"), Mapping):
            for index, item in enumerate(value):
                visit(item, node["items"], f"{path}[{index}]")
        elif isinstance(value, str):
            if len(value) < int(node.get("minLength", 0)):
                raise SchemaValidationError(f"{path}: string is too short")
            if "maxLength" in node and len(value) > int(node["maxLength"]):
                raise SchemaValidationError(f"{path}: string is too long")
            if node.get("format") == "date-time":
                text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
                try:
                    parsed = datetime.fromisoformat(text)
                except ValueError as exc:
                    raise SchemaValidationError(f"{path}: invalid date-time") from exc
                if parsed.tzinfo is None:
                    raise SchemaValidationError(f"{path}: date-time must include timezone")
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            if "minimum" in node and value < node["minimum"]:
                raise SchemaValidationError(f"{path}: value below minimum")
            if "maximum" in node and value > node["maximum"]:
                raise SchemaValidationError(f"{path}: value above maximum")
            if "exclusiveMinimum" in node and value <= node["exclusiveMinimum"]:
                raise SchemaValidationError(f"{path}: value below exclusive minimum")
            if "exclusiveMaximum" in node and value >= node["exclusiveMaximum"]:
                raise SchemaValidationError(f"{path}: value above exclusive maximum")

    visit(instance, schema, "$")


def validate_schema_file(instance: Any, path: Path) -> None:
    with path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)
    validate_json_schema(instance, schema)
