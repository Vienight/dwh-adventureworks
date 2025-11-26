"""
Transformation and SCD logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

from utilities import get_logger

LOGGER = get_logger("transformation")


@dataclass
class SCDDiff:
    inserts: pd.DataFrame
    updates: pd.DataFrame


def detect_scd2_changes(
    current_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    natural_key: str,
    tracked_columns: List[str],
) -> SCDDiff:
    """
    Compare incoming records to current dimension snapshot.
    """
    current_df = current_df.set_index(natural_key)
    incoming_df = incoming_df.set_index(natural_key)

    new_keys = incoming_df.index.difference(current_df.index)
    inserts = incoming_df.loc[new_keys].reset_index()

    joined = incoming_df.join(current_df, lsuffix="_new", rsuffix="_curr", how="inner")

    change_mask = False
    for column in tracked_columns:
        mask = joined[f"{column}_new"] != joined[f"{column}_curr"]
        change_mask = change_mask | mask if isinstance(change_mask, pd.Series) else mask

    updates = joined[change_mask].reset_index()
    LOGGER.info(
        "SCD diff computed: inserts=%s updates=%s",
        len(inserts),
        len(updates),
    )
    return SCDDiff(inserts=inserts, updates=updates)


def build_fact_payload(
    fact_name: str,
    fact_df: pd.DataFrame,
    lookup_maps: Dict[str, Dict[int, int]],
    fk_columns: Dict[str, str],
) -> pd.DataFrame:
    """
    Replace natural keys with surrogate keys for fact loads.
    """
    enriched = fact_df.copy()
    for lookup_name, column in fk_columns.items():
        mapping = lookup_maps.get(lookup_name, {})
        enriched[column] = enriched[column].map(mapping)
    LOGGER.info("Fact payload prepared for %s rows=%s", fact_name, len(enriched))
    return enriched


