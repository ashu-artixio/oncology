# quriousri_indications_import/mondo_db_mapper.py
"""Database persistence utilities for MONDO disease ontology ingestion."""

from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from typing import Dict, Optional, Sequence

import psycopg2
import psycopg2.extras

LOGGER = logging.getLogger("MONDO")


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Fetch environment variable with default fallback."""
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


class MondoDbMapper(AbstractContextManager):
    """Manage persistence of MONDO indications and relationships."""

    NODE_SQL = """
        INSERT INTO public.indications (
            indication_id,
            name,
            description,
            synonyms,
            external_ids,
            depth,
            has_children,
            child_count,
            is_obsolete,
            is_human,
            updated_at
        )
        VALUES (
            %(indication_id)s,
            %(name)s,
            %(description)s,
            %(synonyms)s,
            %(external_ids)s,
            %(depth)s,
            %(has_children)s,
            %(child_count)s,
            %(is_obsolete)s,
            %(is_human)s,
            %(updated_at)s
        )
        ON CONFLICT (indication_id) DO UPDATE
        SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            synonyms = EXCLUDED.synonyms,
            external_ids = EXCLUDED.external_ids,
            depth = EXCLUDED.depth,
            has_children = EXCLUDED.has_children,
            child_count = EXCLUDED.child_count,
            is_obsolete = EXCLUDED.is_obsolete,
            is_human = EXCLUDED.is_human,
            updated_at = COALESCE(EXCLUDED.updated_at, NOW())
        RETURNING id;
    """

    RELATIONSHIP_SQL = """
        INSERT INTO public.indication_relationships (
            parent_indication_id,
            child_indication_id,
            relationship_type,
            depth_difference
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (parent_indication_id, child_indication_id, relationship_type)
        DO NOTHING;
    """

    def __init__(
        self,
        connection=None,
    ) -> None:
        self._owns_connection = connection is None
        if connection is None:
            connection = psycopg2.connect(
                host=_get_env("PG_HOST", "localhost"),
                port=int(_get_env("PG_PORT", "5432")),
                dbname=_get_env("PG_DATABASE", "quriousri_db"),
                user=_get_env("PG_USER", "postgres"),
                password=_get_env("PG_PASSWORD", "postgres"),
            )
        self.connection = connection
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.cursor.close()
        if self._owns_connection:
            self.connection.close()

    def upsert_term(
        self,
        record: Dict,
        child_presence_lookup: Dict[str, bool],
    ) -> int:
        """Insert or update a MONDO indication."""
        child_ids = record.get("child_ids") or []
        present_children = [child for child in child_ids if child_presence_lookup.get(child)]
        params = {
            "indication_id": record["indication_id"],
            "name": record["name"],
            "description": record.get("description"),
            "synonyms": record.get("synonyms") or [],
            "external_ids": psycopg2.extras.Json(record.get("external_ids") or {}),
            "depth": record.get("depth", -1),
            "has_children": bool(present_children),
            "child_count": len(present_children),
            "is_obsolete": record.get("is_obsolete", False),
            "is_human": record.get("is_human", True),
            "updated_at": record.get("updated_at"),
        }
        self.cursor.execute(self.NODE_SQL, params)
        inserted = self.cursor.fetchone()
        return int(inserted["id"])

    def sync_relationships(
        self,
        child_db_id: int,
        parent_mondo_ids: Sequence[str],
        indicator_lookup: Dict[str, int],
        relationship_type: str = "is_a",
        child_depth: Optional[int] = None,
    ) -> int:
        """Ensure parent-child relationships exist for a given node."""
        created = 0
        if child_depth is None or child_depth <= 0:
            return created
        expected_parent_depth = child_depth - 1
        for parent_mondo_id in parent_mondo_ids:
            parent_db_id = indicator_lookup.get(parent_mondo_id)
            if not parent_db_id:
                continue
            if expected_parent_depth >= 0:
                self._ensure_depth(parent_db_id, expected_parent_depth)
            current_child_depth = self._get_depth(child_db_id)
            if current_child_depth is None or current_child_depth != child_depth:
                self._ensure_depth(child_db_id, child_depth)
            self.cursor.execute(
                self.RELATIONSHIP_SQL,
                (parent_db_id, child_db_id, relationship_type, 1),
            )
            if self.cursor.rowcount:
                created += 1
        return created

    def fetch_ids_for_mondo(self, mondo_ids: Sequence[str]) -> Dict[str, int]:
        """Resolve MONDO IDs to database primary keys."""
        mondo_ids = [mid for mid in mondo_ids if mid]
        if not mondo_ids:
            return {}
        self.cursor.execute(
            """
            SELECT indication_id, id
            FROM public.indications
            WHERE indication_id = ANY(%s)
            """,
            (list(set(mondo_ids)),),
        )
        rows = self.cursor.fetchall() or []
        return {row["indication_id"]: int(row["id"]) for row in rows}

    def cleanup_stale_relationships(
        self,
        child_db_id: int,
        allowed_parent_ids: Sequence[int],
    ) -> None:
        """Remove parent relationships that are no longer present."""
        allowed_parent_ids = list(allowed_parent_ids)
        if not allowed_parent_ids:
            self.cursor.execute(
                "DELETE FROM public.indication_relationships WHERE child_indication_id = %s",
                (child_db_id,),
            )
            return

        placeholders = ",".join(["%s"] * len(allowed_parent_ids))
        query = f"""
            DELETE FROM public.indication_relationships
            WHERE child_indication_id = %s
              AND parent_indication_id NOT IN ({placeholders})
        """
        params = (child_db_id, *allowed_parent_ids)
        self.cursor.execute(query, params)

    def commit_batch(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def _ensure_depth(self, db_id: int, expected_depth: int) -> None:
        """Force parent depth to expected value when it has diverged."""
        if expected_depth < 0:
            return
        self.cursor.execute(
            """
            UPDATE public.indications
            SET depth = %s
            WHERE id = %s
            """,
            (expected_depth, db_id),
        )

    def _get_depth(self, db_id: int) -> Optional[int]:
        self.cursor.execute(
            "SELECT depth FROM public.indications WHERE id = %s",
            (db_id,),
        )
        row = self.cursor.fetchone()
        if not row:
            return None
        depth = row.get("depth")
        return int(depth) if depth is not None else None

