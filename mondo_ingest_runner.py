# quriousri_indications_import/mondo_ingest_runner.py
"""Coordinate the MONDO ingestion workflow from fetch to persistence."""

from __future__ import annotations

import logging
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence

if __package__ in (None, ""):
    import sys as _sys

    _CURRENT_DIR = Path(__file__).resolve().parent
    if str(_CURRENT_DIR) not in _sys.path:
        _sys.path.insert(0, str(_CURRENT_DIR))

    from mondo_db_mapper import MondoDbMapper  # type: ignore
    from mondo_fetcher import (  # type: ignore
        DEFAULT_SOURCE_URL,
        ensure_mondo_json,
        extract_version,
    )
    from mondo_normalizer import attach_relationships  # type: ignore
    from mondo_normalizer import build_relationship_index, compute_depths, flatten_nodes  # type: ignore
else:
    from .mondo_db_mapper import MondoDbMapper
    from .mondo_fetcher import DEFAULT_SOURCE_URL, ensure_mondo_json, extract_version
    from .mondo_normalizer import (
        attach_relationships,
        build_relationship_index,
        compute_depths,
        flatten_nodes,
    )

LOGGER = logging.getLogger("MONDO")


def _parse_updated_since(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO-8601 timestamps with basic timezone support."""
    if not value:
        return None

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        raise ValueError(f"Unable to parse '--updated-since' value: {value}")


def _chunk_records(records: Sequence[Dict], size: int) -> Iterator[List[Dict]]:
    """Yield slices of a list in fixed-size batches."""
    it = iter(records)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


class MondoIngestRunner:
    """High-level ingestion pipeline for MONDO ontology data."""

    def __init__(self, module_config: Dict) -> None:
        self.config = module_config
        self.source_url = module_config.get("source_url", DEFAULT_SOURCE_URL)
        self.cache_dir = Path(module_config.get("cache_dir") or "temp/mondo_cache")
        self.force_refresh = bool(module_config.get("force_refresh", False))
        self.batch_size = int(module_config.get("batch_size", 500))
        self.human_only = bool(module_config.get("human_only", True))

    def run(self, updated_since: Optional[str] = None) -> None:
        """Execute the ingestion pipeline."""
        updated_since_dt = _parse_updated_since(updated_since)

        cache_path, payload = ensure_mondo_json(
            cache_dir=self.cache_dir,
            source_url=self.source_url,
            force_refresh=self.force_refresh,
        )
        release_version = extract_version(payload)
        graph = (payload.get("graphs") or [{}])[0]

        LOGGER.info(
            "Prepared MONDO payload at %s (version=%s)",
            cache_path,
            release_version,
        )

        parents, children = build_relationship_index(graph)
        normalized_nodes = list(
            flatten_nodes(
                graph,
                release_version=release_version,
                human_only=self.human_only,
            )
        )
        attach_relationships(normalized_nodes, parents, children)
        depth_lookup = compute_depths(parents, [node["indication_id"] for node in normalized_nodes])
        for node in normalized_nodes:
            node["depth"] = depth_lookup.get(node["indication_id"], -1)

        normalized_nodes.sort(
            key=lambda item: (
                item.get("depth", -1),
                item["indication_id"],
            )
        )

        if updated_since_dt:
            before_count = len(normalized_nodes)
            normalized_nodes = [
                node
                for node in normalized_nodes
                if not node.get("updated_at") or node["updated_at"] >= updated_since_dt
            ]
            LOGGER.info(
                "Filtered nodes by updated_since=%s (kept %s of %s)",
                updated_since_dt.isoformat(),
                len(normalized_nodes),
                before_count,
            )

        if not normalized_nodes:
            LOGGER.info("No MONDO nodes to ingest after filtering.")
            return

        child_presence_lookup = {node["indication_id"]: True for node in normalized_nodes}
        mondo_id_lookup: Dict[str, int] = {}

        stats = {
            "nodes_processed": 0,
            "relationships_created": 0,
        }

        with MondoDbMapper() as mapper:
            batches = list(_chunk_records(normalized_nodes, self.batch_size))
            for index, batch in enumerate(batches, start=1):
                LOGGER.info("Processing batch %s/%s (%s nodes)", index, len(batches), len(batch))
                for node in batch:
                    db_id = mapper.upsert_term(node, child_presence_lookup)
                    mondo_id_lookup[node["indication_id"]] = db_id
                    stats["nodes_processed"] += 1

                parent_ids_needed = set()
                for node in batch:
                    parent_ids_needed.update(node.get("parent_ids") or [])
                missing = [mid for mid in parent_ids_needed if mid not in mondo_id_lookup]
                if missing:
                    mondo_id_lookup.update(mapper.fetch_ids_for_mondo(missing))

                for node in batch:
                    child_db_id = mondo_id_lookup.get(node["indication_id"])
                    if not child_db_id:
                        continue
                    parent_mondo_ids = [
                        pid for pid in node.get("parent_ids", []) if pid in mondo_id_lookup
                    ]
                    created = mapper.sync_relationships(
                        child_db_id=child_db_id,
                        parent_mondo_ids=parent_mondo_ids,
                        indicator_lookup=mondo_id_lookup,
                        child_depth=node.get("depth"),
                    )
                    mapper.cleanup_stale_relationships(
                        child_db_id,
                        [mondo_id_lookup[pid] for pid in parent_mondo_ids],
                    )
                    stats["relationships_created"] += created

                mapper.commit_batch()

        LOGGER.info(
            "MONDO ingestion complete: %s nodes processed, %s relationships created",
            stats["nodes_processed"],
            stats["relationships_created"],
        )

