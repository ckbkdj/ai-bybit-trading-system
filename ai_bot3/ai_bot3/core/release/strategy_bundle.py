from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from contracts.strategy_release_v1 import StrategyReleaseBundle


class StrategyReleaseVerificationError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_payload(bundle: StrategyReleaseBundle) -> dict[str, Any]:
    payload = bundle.model_dump(mode="json")
    payload.pop("bundle_sha256", None)
    return payload


def canonical_bundle_hash(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("bundle_sha256", None)
    normalized.setdefault("schema_version", "strategy-release-bundle.v1")
    normalized.setdefault("artifact_paths", {})
    normalized.setdefault("immutable_limits", {})
    text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class StrategyReleaseLoader:
    """Verify a release manifest and every declared artifact as one atomic unit."""

    @staticmethod
    def load(path: Path) -> StrategyReleaseBundle:
        manifest_path = Path(path).resolve()
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        bundle = StrategyReleaseBundle.model_validate(raw)
        expected_manifest_hash = canonical_bundle_hash(_manifest_payload(bundle))
        if expected_manifest_hash != bundle.bundle_sha256:
            raise StrategyReleaseVerificationError("strategy release manifest hash mismatch")
        artifact_hashes = bundle.artifacts.model_dump()
        if set(bundle.artifact_paths) != set(artifact_hashes):
            raise StrategyReleaseVerificationError(
                "a deployable strategy release must declare a path for every artifact hash"
            )
        for key, relative in bundle.artifact_paths.items():
            artifact_path = (manifest_path.parent / relative).resolve()
            if manifest_path.parent not in artifact_path.parents:
                raise StrategyReleaseVerificationError(f"artifact path escapes bundle root: {key}")
            if not artifact_path.is_file():
                raise StrategyReleaseVerificationError(f"strategy artifact is missing: {key}")
            if _sha256_file(artifact_path) != artifact_hashes[key]:
                raise StrategyReleaseVerificationError(f"strategy artifact hash mismatch: {key}")
        return bundle

    @staticmethod
    def effective_limits(
        bundle: StrategyReleaseBundle, account_limits: Mapping[str, float]
    ) -> dict[str, float]:
        """Account configuration may tighten, but never loosen, release limits."""

        effective = dict(bundle.immutable_limits)
        for name, configured in account_limits.items():
            if name not in effective:
                effective[name] = float(configured)
            else:
                effective[name] = min(float(effective[name]), float(configured))
        return effective
