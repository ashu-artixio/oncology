# quriousri_indications_import/mondo_filter.py
"""Filter MONDO ontology nodes based on exclusion criteria and hierarchy relationships."""

from __future__ import annotations

import logging
from collections import deque
from typing import Dict, List, Optional, Set

LOGGER = logging.getLogger("MONDO")


def get_all_descendants(
    node_id: str,
    children: Dict[str, Set[str]],
) -> Set[str]:
    """Get all descendants of a given node using BFS traversal."""
    descendants = set()
    queue = deque([node_id])
    visited = {node_id}

    while queue:
        current = queue.popleft()
        descendants.add(current)

        # Find all children of current node
        for child in children.get(current, set()):
            if child not in visited:
                visited.add(child)
                queue.append(child)

    return descendants


def get_all_ancestors(
    node_id: str,
    parents: Dict[str, Set[str]],
) -> Set[str]:
    """Get all ancestors of a given node using BFS traversal."""
    ancestors = set()
    queue = deque([node_id])
    visited = {node_id}

    while queue:
        current = queue.popleft()
        ancestors.add(current)

        # Find all parents of current node
        for parent in parents.get(current, set()):
            if parent not in visited:
                visited.add(parent)
                queue.append(parent)

    return ancestors


def should_remove(
    indication_id: str,
    target_ids: Set[str],
    parents: Optional[Dict[str, Set[str]]] = None,
    children: Optional[Dict[str, Set[str]]] = None,
) -> bool:
    """Check if indication should be removed based on connection to target IDs."""
    if not indication_id:
        return False

    # Normalize indication_id (handle both MONDO:123 and MONDO_123 formats)
    normalized_id = indication_id.replace(":", "_")

    # Direct match check
    if normalized_id in target_ids:
        return True

    # If no hierarchy data, only check exact matches
    if not parents or not children:
        # Also check if any target ID is a substring (for partial matches)
        return any(target_id in normalized_id or normalized_id in target_id for target_id in target_ids)

    # Get all related IDs (ancestors + descendants) for all target IDs
    related_ids = set()
    for target_id in target_ids:
        related_ids.update(get_all_descendants(target_id, children))
        related_ids.update(get_all_ancestors(target_id, parents))
        related_ids.add(target_id)  # Include the target itself

    return normalized_id in related_ids


def filter_nodes(
    nodes: List[Dict],
    exclude_mondo_ids: List[str],
    parents: Optional[Dict[str, Set[str]]] = None,
    children: Optional[Dict[str, Set[str]]] = None,
) -> List[Dict]:
    """Filter out nodes that match or are related to excluded MONDO IDs."""
    if not exclude_mondo_ids:
        return nodes

    # Normalize target IDs (handle both MONDO:123 and MONDO_123 formats)
    target_ids = {mid.replace(":", "_") for mid in exclude_mondo_ids}
    
    LOGGER.debug("Normalized target IDs for filtering: %s", target_ids)

    before_count = len(nodes)
    filtered_nodes = []
    removed_ids = set()

    for node in nodes:
        indication_id = node.get("indication_id") or node.get("mondo_id") or node.get("id")
        if not indication_id:
            # Keep nodes without IDs (shouldn't happen, but be safe)
            filtered_nodes.append(node)
            continue
            
        if should_remove(indication_id, target_ids, parents, children):
            removed_ids.add(indication_id)
        else:
            filtered_nodes.append(node)

    removed_count = before_count - len(filtered_nodes)
    if removed_count > 0:
        LOGGER.info(
            "Filtered out %s nodes related to excluded MONDO IDs: %s",
            removed_count,
            ", ".join(exclude_mondo_ids),
        )
        if len(removed_ids) <= 20:  # Only log if not too many
            LOGGER.debug("Removed indication IDs (sample): %s", list(removed_ids)[:20])
    else:
        LOGGER.warning(
            "No nodes were filtered. Check if exclude_mondo_ids are correct: %s",
            ", ".join(exclude_mondo_ids),
        )

    return filtered_nodes

