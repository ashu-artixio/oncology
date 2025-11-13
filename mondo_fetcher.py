# quriousri_indications_import/mondo_fetcher.py
"""Download and cache MONDO ontology artifacts."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
from requests import Response

LOGGER = logging.getLogger("MONDO")
DEFAULT_SOURCE_URL = "http://purl.obolibrary.org/obo/mondo.json"


def _compute_sha256(file_path: Path) -> str:
    """Calculate the SHA-256 checksum of a file."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def download_mondo_json(
    url: str,
    target_path: Path,
    retries: int = 3,
    timeout: int = 60,
) -> Path:
    """Stream MONDO JSON to disk with retries and optional ETag caching."""
    session = requests.Session()
    etag_path = target_path.with_suffix(target_path.suffix + ".etag")
    headers = {"Accept": "application/json"}

    if etag_path.exists():
        headers["If-None-Match"] = etag_path.read_text(encoding="utf-8").strip()

    for attempt in range(1, retries + 1):
        try:
            LOGGER.info("MONDO download attempt %s from %s", attempt, url)
            response: Response = session.get(
                url,
                stream=True,
                timeout=timeout,
                headers=headers,
            )

            if response.status_code == 304:
                LOGGER.info("ETag indicated cached MONDO payload is current.")
                return target_path

            response.raise_for_status()

            temporary_path = target_path.with_suffix(".tmp")
            with temporary_path.open("wb") as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)

            temporary_path.replace(target_path)

            if response.headers.get("ETag"):
                etag_path.write_text(response.headers["ETag"], encoding="utf-8")

            checksum = _compute_sha256(target_path)
            LOGGER.info(
                "Downloaded MONDO payload (%s bytes, sha256=%s)",
                target_path.stat().st_size,
                checksum,
            )
            return target_path
        except Exception as exc:
            sleep_seconds = min(2**attempt, 30)
            LOGGER.warning(
                "Download attempt %s failed: %s (retrying in %ss)",
                attempt,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to download MONDO ontology after {retries} attempts.")


def load_cached_mondo_json(path: Path) -> Dict[str, Any]:
    """Load MONDO ontology JSON into memory with basic validation."""
    if not path.exists():
        raise FileNotFoundError(f"Cached MONDO file not found at {path}")

    LOGGER.info("Loading MONDO ontology from %s", path)
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    if "graphs" not in payload or not payload["graphs"]:
        raise ValueError("Invalid MONDO payload: expected 'graphs[0]' to exist.")

    return payload


def extract_version(metadata: Dict[str, Any]) -> str:
    """Extract release version value from MONDO metadata."""
    graphs = metadata.get("graphs") or []
    if not graphs:
        return "unknown"

    first_graph_meta = graphs[0].get("meta") or {}
    version = first_graph_meta.get("version")
    if isinstance(version, str) and version.strip():
        return version.strip()

    return first_graph_meta.get("versionInfo") or "unknown"


def ensure_mondo_json(
    cache_dir: Path,
    source_url: str = DEFAULT_SOURCE_URL,
    force_refresh: bool = False,
) -> Tuple[Path, Dict[str, Any]]:
    """Ensure the MONDO JSON exists locally and return its parsed content."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "mondo.json"

    if force_refresh or not cache_path.exists():
        download_mondo_json(source_url, cache_path)

    payload = load_cached_mondo_json(cache_path)
    return cache_path, payload

