# quriousri_indications_import/mondo_normalizer.py
"""Normalize MONDO ontology JSON into database-ready structures."""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

LOGGER = logging.getLogger("MONDO")

MONDO_ID_PATTERN = re.compile(r"MONDO[_:]\d+")
NCBITAXON_PATTERN = re.compile(r"NCBITaxon[_:](\d+)")


@dataclass(frozen=True)
class HumanClassification:
    """Represents a human/animal classification decision."""

    is_human: bool
    confidence: float
    reason: str


def extract_mondo_id(raw_identifier: Optional[str]) -> Optional[str]:
    """Extract canonical MONDO ID from URI or CURIE."""
    if not raw_identifier:
        return None

    match = MONDO_ID_PATTERN.search(raw_identifier)
    if not match:
        return None

    value = match.group(0)
    return value.replace(":", "_")


def classify_human_indication(node: Dict) -> HumanClassification:
    """Determine whether a MONDO node represents a human disease."""
    meta = node.get("meta") or {}
    name = (node.get("lbl") or "").lower()
    xrefs = meta.get("xrefs") or []
    properties = meta.get("basicPropertyValues") or []

    explicit_non_human_terms = {
        "non-human animal",
        "non-human",
        "nonhuman animal",
        "nonhuman",
    }
    if any(term in name for term in explicit_non_human_terms):
        return HumanClassification(False, 0.0, "Explicit non-human label")

    exclude_terms = {
        "veterinary",
        "plant disease",
        "animal disease",
        "livestock",
        "cattle",
        "swine",
        "poultry",
        "bovine",
        "canine",
        "feline",
        "equine",
        "porcine",
        "avian",
        "ovine",
        "caprine",
    }
    if any(term in name for term in exclude_terms):
        return HumanClassification(False, 0.0, "Veterinary or animal disease term")

    for prop in properties:
        val = prop.get("val") or ""
        match = NCBITAXON_PATTERN.search(val)
        if match and match.group(1) != "9606":
            return HumanClassification(False, 0.0, "Non-human taxon detected")

    if any((xref.get("val") or "").startswith("OMIA:") for xref in xrefs):
        return HumanClassification(False, 0.0, "Veterinary OMIA cross-reference")

    for prop in properties:
        val = prop.get("val") or ""
        if "NCBITaxon_9606" in val:
            return HumanClassification(True, 1.0, "Explicit Homo sapiens taxon")

    high_confidence_prefixes = (
        "ICD10CM:",
        "ICD10:",
        "ICD9CM:",
        "ICD9:",
        "SNOMEDCT_US:",
        "SNOMEDCT:",
        "Orphanet:",
        "ORDO:",
        "OMIM:",
        "MIM:",
        "GARD:",
        "NCIT:",
        "DOID:",
    )
    if any(
        (xref.get("val") or "").startswith(prefix)
        for prefix in high_confidence_prefixes
        for xref in xrefs
    ):
        return HumanClassification(True, 0.95, "High-confidence clinical coding xref")

    medium_confidence_prefixes = ("UMLS:", "MESH:", "MSH:")
    if any(
        (xref.get("val") or "").startswith(prefix)
        for prefix in medium_confidence_prefixes
        for xref in xrefs
    ):
        return HumanClassification(True, 0.8, "Medical ontology cross-reference")

    definition = (meta.get("definition", {}).get("val") or "").lower()
    if any(term in definition for term in ("in humans", "human disease", "affects humans")):
        return HumanClassification(True, 0.85, "Definition references humans")

    return HumanClassification(True, 0.7, "Default assumption for MONDO disease ontology")


def parse_synonyms(meta: Dict, limit: int = 50) -> List[str]:
    """Extract normalized synonym strings from metadata."""
    synonyms = meta.get("synonyms") or []
    values = []
    for synonym in synonyms:
        value = (synonym.get("val") or "").strip()
        if value:
            values.append(value)
        if len(values) >= limit:
            break
    return values


def parse_external_ids(meta: Dict) -> Dict[str, List[str]]:
    """Transform xrefs into a prefix -> values mapping."""
    external = defaultdict(list)
    for xref in meta.get("xrefs") or []:
        value = xref.get("val") or ""
        if ":" not in value:
            continue
        prefix, identifier = value.split(":", 1)
        if identifier:
            external[prefix].append(identifier)
    return dict(external)


def extract_updated_timestamp(node: Dict) -> Optional[datetime]:
    """Best-effort extraction of the node's last modified timestamp."""
    meta = node.get("meta") or {}
    history = meta.get("basicPropertyValues") or []
    for prop in history:
        pred = prop.get("pred") or ""
        if pred.endswith("dcterms/date") or pred.endswith("dcterms:date"):
            try:
                return datetime.fromisoformat(prop.get("val"))
            except (TypeError, ValueError):
                continue

    created = meta.get("createdOn")
    if created:
        try:
            return datetime.fromisoformat(created)
        except ValueError:
            pass

    return None


def build_relationship_index(
    graph: Dict,
) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
    """Build parent/child lookup structures from graph edges."""
    parents: Dict[str, Set[str]] = defaultdict(set)
    children: Dict[str, Set[str]] = defaultdict(set)

    for edge in graph.get("edges") or []:
        child = extract_mondo_id(edge.get("sub"))
        parent = extract_mondo_id(edge.get("obj"))
        predicate = edge.get("pred") or ""

        if not child or not parent:
            continue

        if not predicate.endswith("is_a") and not predicate.endswith("is_a>"):
            continue

        parents[child].add(parent)
        children[parent].add(child)

    return parents, children


def normalize_node(
    node: Dict,
    release_version: str,
    human_only: bool = True,
) -> Optional[Dict]:
    """Normalize a single MONDO node into the ingestion schema."""
    mondo_id = extract_mondo_id(node.get("id"))
    if not mondo_id:
        return None

    classification = classify_human_indication(node)
    if human_only and not classification.is_human:
        return None

    meta = node.get("meta") or {}
    name = node.get("lbl") or ""
    
    # Skip nodes with empty or null names
    if not name or not name.strip():
        return None

    normalized = {
        "indication_id": mondo_id,
        "name": name,
        "description": meta.get("definition", {}).get("val"),
        "synonyms": parse_synonyms(meta),
        "external_ids": parse_external_ids(meta),
        "entity_type": (node.get("type") or [""])[0],
        "release_version": release_version,
        "is_obsolete": bool(meta.get("deprecated") or node.get("deprecated")),
        "is_human": classification.is_human,
        "human_reason": classification.reason,
        "human_confidence": classification.confidence,
        "updated_at": extract_updated_timestamp(node),
    }
    return normalized


def flatten_nodes(
    graph: Dict,
    release_version: str,
    human_only: bool = True,
) -> Iterable[Dict]:
    """Yield normalized nodes for ingestion."""
    total_nodes = 0
    skipped_empty_name = 0
    
    for node in graph.get("nodes") or []:
        total_nodes += 1
        
        # Check for empty name before normalization (for accurate counting)
        name = node.get("lbl") or ""
        if not name or not name.strip():
            skipped_empty_name += 1
            continue  # Skip this node, don't even try to normalize
        
        normalized = normalize_node(node, release_version, human_only=human_only)
        if normalized:
            yield normalized
    
    # Log statistics if any nodes were skipped due to empty names
    if skipped_empty_name > 0:
        LOGGER.info(
            "Skipped %s nodes with empty or null names (out of %s total nodes)",
            skipped_empty_name,
            total_nodes,
        )


def attach_relationships(
    nodes: Sequence[Dict],
    parents: Dict[str, Set[str]],
    children: Dict[str, Set[str]],
) -> None:
    """Enrich normalized nodes with parent/child MONDO IDs."""
    node_index = {node["indication_id"]: node for node in nodes}
    for indication_id, node in node_index.items():
        node["parent_ids"] = sorted(parents.get(indication_id, ()))
        node["child_ids"] = sorted(children.get(indication_id, ()))


def compute_depths(
    parents: Dict[str, Set[str]],
    node_ids: Iterable[str],
    max_iterations: int = 5_000,
) -> Dict[str, int]:
    """Compute hierarchical depth for MONDO nodes based on parent relationships."""
    depths: Dict[str, int] = {}
    remaining = set(node_ids)
    iterations = 0

    while remaining and iterations < max_iterations:
        resolved_this_round = set()
        for node_id in list(remaining):
            parent_ids = parents.get(node_id, set())
            if not parent_ids:
                depths[node_id] = 0
                resolved_this_round.add(node_id)
                continue

            parent_depths = [depths[parent] for parent in parent_ids if parent in depths]
            if parent_depths:
                depths[node_id] = min(parent_depths) + 1
                resolved_this_round.add(node_id)

        remaining -= resolved_this_round
        if not resolved_this_round:
            break
        iterations += 1

    return depths

