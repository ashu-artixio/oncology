# quriousri_indications_import/therapeutic_area_mapper.py
"""AI-powered mapping of indications to therapeutic areas using OpenAI."""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional, Sequence, Set

import psycopg2
import psycopg2.extras

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

LOGGER = logging.getLogger("MONDO")


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Fetch environment variable with default fallback."""
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


class TherapeuticAreaMapper:
    """Map indications to therapeutic areas using AI-powered classification."""

    def __init__(
        self,
        connection=None,
        openai_api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        batch_size: int = 50,
    ) -> None:
        """
        Initialize the therapeutic area mapper.
        
        Args:
            connection: PostgreSQL connection (creates new if None)
            openai_api_key: OpenAI API key (uses OPENAI_API_KEY env var if None)
            model: OpenAI model to use (default: gpt-4o-mini)
            batch_size: Number of indications to process per API call
        """
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
        
        # Initialize OpenAI client
        api_key = openai_api_key or _get_env("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OpenAI API key is required. Set OPENAI_API_KEY environment variable or pass openai_api_key parameter."
            )
        
        if OpenAI is None:
            raise ImportError(
                "OpenAI package is required. Install it with: pip install openai"
            )
        
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.batch_size = batch_size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.cursor.close()
        if self._owns_connection:
            self.connection.close()

    def fetch_therapeutic_areas(self) -> List[Dict]:
        """Fetch all active therapeutic areas from the database."""
        self.cursor.execute(
            """
            SELECT id, name, description, domain
            FROM public.therapeutic_areas
            WHERE is_active = TRUE
            ORDER BY name
            """
        )
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_indications_to_map(
        self,
        indication_ids: Optional[Sequence[int]] = None,
        only_new_or_updated: bool = True,
    ) -> List[Dict]:
        """
        Fetch indications that need therapeutic area mapping.
        
        Args:
            indication_ids: Specific indication IDs to map (None = all)
            only_new_or_updated: Only fetch indications without existing mappings
        """
        if indication_ids:
            placeholders = ",".join(["%s"] * len(indication_ids))
            query = f"""
                SELECT i.id, i.indication_id, i.name, i.description, i.synonyms
                FROM public.indications i
                WHERE i.id IN ({placeholders})
                ORDER BY i.name
            """
            self.cursor.execute(query, list(indication_ids))
        else:
            if only_new_or_updated:
                # Only fetch indications that don't have therapeutic area mappings
                query = """
                    SELECT DISTINCT i.id, i.indication_id, i.name, i.description, i.synonyms
                    FROM public.indications i
                    LEFT JOIN public.indication_therapeutic_areas ita ON i.id = ita.indication_id
                    WHERE ita.id IS NULL
                    ORDER BY i.name
                """
            else:
                query = """
                    SELECT i.id, i.indication_id, i.name, i.description, i.synonyms
                    FROM public.indications i
                    ORDER BY i.name
                """
            self.cursor.execute(query)
        
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def _build_mapping_prompt(
        self,
        indications: List[Dict],
        therapeutic_areas: List[Dict],
    ) -> str:
        """Build the prompt for OpenAI to map indications to therapeutic areas."""
        ta_list = "\n".join(
            [
                f"- {ta['id']}: {ta['name']}"
                + (f" ({ta['description']})" if ta.get("description") else "")
                + (f" [Domain: {ta['domain']}]" if ta.get("domain") else "")
                for ta in therapeutic_areas
            ]
        )
        
        indication_list = "\n".join(
            [
                f"- ID {ind['id']} ({ind['indication_id']}): {ind['name']}"
                + (f" - {ind['description']}" if ind.get("description") else "")
                + (
                    f" [Synonyms: {', '.join(ind['synonyms'][:5])}]"
                    if ind.get("synonyms")
                    else ""
                )
                for ind in indications
            ]
        )
        
        prompt = f"""You are a medical classification expert. Your task is to map disease indications to therapeutic areas.

Therapeutic Areas Available:
{ta_list}

Indications to Map:
{indication_list}

Instructions:
1. Analyze each indication carefully based on its name, description, and synonyms
2. Map each indication to one or more appropriate therapeutic areas by their ID
3. A single indication can belong to multiple therapeutic areas (many-to-many relationship)
4. Be precise and only map when there is a clear medical/therapeutic relationship
5. Return your response as a JSON array of objects, each with:
   - "indication_id": the indication's database ID (integer)
   - "therapeutic_area_ids": array of therapeutic area IDs (integers) that this indication belongs to

Example response format (return this exact structure as a JSON array):
[
  {{"indication_id": 1, "therapeutic_area_ids": [2, 5]}},
  {{"indication_id": 2, "therapeutic_area_ids": [3]}},
  {{"indication_id": 3, "therapeutic_area_ids": [1, 2, 4]}}
]

CRITICAL: Return ONLY a valid JSON array starting with [ and ending with ]. No markdown code blocks, no explanations, no additional text."""
        
        return prompt

    def _call_openai_for_mapping(
        self,
        prompt: str,
    ) -> List[Dict]:
        """Call OpenAI API to get therapeutic area mappings."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a medical classification expert specializing in mapping diseases to therapeutic areas. You MUST respond with ONLY a valid JSON array, no markdown, no code blocks, no explanations. The response must start with [ and end with ].",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,  # Lower temperature for more consistent classifications
            )
            
            content = response.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            if content.startswith("```"):
                lines = content.split("\n")
                # Remove first line (```json or ```)
                if len(lines) > 1:
                    lines = lines[1:]
                # Remove last line (```)
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines).strip()
            
            # Try to parse as JSON array
            try:
                parsed = json.loads(content)
                if isinstance(parsed, list):
                    return parsed
                elif isinstance(parsed, dict):
                    # If it's a dict, try to extract array from common keys
                    for key in ["mappings", "results", "data", "items"]:
                        if key in parsed and isinstance(parsed[key], list):
                            return parsed[key]
                    # If no array found, wrap in list
                    return [parsed]
            except json.JSONDecodeError:
                # Try to extract JSON array from text using regex
                import re
                json_match = re.search(r'\[.*\]', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(0))
                # Try to find JSON object and wrap it
                obj_match = re.search(r'\{.*\}', content, re.DOTALL)
                if obj_match:
                    return [json.loads(obj_match.group(0))]
                LOGGER.warning(f"Could not parse JSON from response: {content[:200]}")
                raise
            
            return []
        except Exception as e:
            LOGGER.error(f"Error calling OpenAI API: {e}")
            raise

    def map_indications_to_therapeutic_areas(
        self,
        indication_ids: Optional[Sequence[int]] = None,
        only_new_or_updated: bool = True,
    ) -> Dict[str, int]:
        """
        Map indications to therapeutic areas using AI.
        
        Returns statistics dictionary with counts of mappings created.
        """
        therapeutic_areas = self.fetch_therapeutic_areas()
        if not therapeutic_areas:
            LOGGER.warning("No therapeutic areas found in database. Skipping mapping.")
            return {"mappings_created": 0, "indications_processed": 0, "errors": 0}
        
        LOGGER.info(f"Found {len(therapeutic_areas)} therapeutic areas")
        
        indications = self.fetch_indications_to_map(
            indication_ids=indication_ids,
            only_new_or_updated=only_new_or_updated,
        )
        
        if not indications:
            LOGGER.info("No indications to map")
            return {"mappings_created": 0, "indications_processed": 0, "errors": 0}
        
        LOGGER.info(f"Mapping {len(indications)} indications to therapeutic areas")
        
        # Create lookup for therapeutic area IDs
        ta_id_set = {ta["id"] for ta in therapeutic_areas}
        
        stats = {
            "mappings_created": 0,
            "indications_processed": 0,
            "errors": 0,
        }
        
        # Process in batches
        for i in range(0, len(indications), self.batch_size):
            batch = indications[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(indications) + self.batch_size - 1) // self.batch_size
            
            LOGGER.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} indications)"
            )
            
            try:
                prompt = self._build_mapping_prompt(batch, therapeutic_areas)
                mappings = self._call_openai_for_mapping(prompt)
                
                # Validate and apply mappings
                for mapping in mappings:
                    indication_id = mapping.get("indication_id")
                    ta_ids = mapping.get("therapeutic_area_ids", [])
                    
                    if not indication_id or not isinstance(ta_ids, list):
                        continue
                    
                    # Validate therapeutic area IDs
                    valid_ta_ids = [ta_id for ta_id in ta_ids if ta_id in ta_id_set]
                    
                    if not valid_ta_ids:
                        continue
                    
                    # Insert mappings
                    for ta_id in valid_ta_ids:
                        try:
                            self.cursor.execute(
                                """
                                INSERT INTO public.indication_therapeutic_areas
                                    (indication_id, therapeutic_area_id)
                                VALUES (%s, %s)
                                ON CONFLICT (indication_id, therapeutic_area_id) DO NOTHING
                                """,
                                (indication_id, ta_id),
                            )
                            if self.cursor.rowcount > 0:
                                stats["mappings_created"] += 1
                        except Exception as e:
                            LOGGER.error(
                                f"Error inserting mapping for indication {indication_id} to therapeutic area {ta_id}: {e}"
                            )
                            stats["errors"] += 1
                
                stats["indications_processed"] += len(batch)
                self.connection.commit()
                
            except Exception as e:
                LOGGER.error(f"Error processing batch {batch_num}: {e}")
                stats["errors"] += 1
                self.connection.rollback()
                continue
        
        LOGGER.info(
            f"Mapping complete: {stats['indications_processed']} indications processed, "
            f"{stats['mappings_created']} mappings created, {stats['errors']} errors"
        )
        
        return stats

