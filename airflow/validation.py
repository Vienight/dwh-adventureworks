"""
Data quality validation utilities.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

from utilities import get_logger

LOGGER = get_logger("validation")


ValidationResult = Tuple[str, bool, str]


def validate_extracted_data(frames: Dict[str, pd.DataFrame]) -> List[ValidationResult]:
    """
    Run column-level checks on extracted dataframes.
    """
    results: List[ValidationResult] = []
    for name, df in frames.items():
        tests = [
            _check_nulls(name, df),
            _check_duplicates(name, df),
            _check_numeric_ranges(name, df),
        ]
        for test in tests:
            if test:
                results.append(test)

    failures = [result for result in results if not result[1]]
    if failures:
        LOGGER.warning("Validation failures detected: %s", failures)
    else:
        LOGGER.info("All validation checks passed")
    return results


def _check_nulls(name: str, df: pd.DataFrame) -> ValidationResult:
    null_counts = df.isna().sum()
    offending = {col: int(count) for col, count in null_counts.items() if count > 0}
    status = len(offending) == 0
    message = f"Null counts={offending}" if offending else "No nulls detected"
    return (f"{name}_nulls", status, message)


def _check_duplicates(name: str, df: pd.DataFrame) -> ValidationResult:
    if df.empty:
        return (f"{name}_duplicates", True, "empty dataframe")
    dup_count = int(df.duplicated().sum())
    status = dup_count == 0
    message = f"{dup_count} duplicates detected" if dup_count else "No duplicates"
    return (f"{name}_duplicates", status, message)


def _check_numeric_ranges(name: str, df: pd.DataFrame) -> ValidationResult:
    numeric_cols = df.select_dtypes(include=["number"]).columns
    negatives = {
        col: int((df[col] < 0).sum())
        for col in numeric_cols
        if (df[col] < 0).any()
    }
    status = len(negatives) == 0
    message = f"Negative values {negatives}" if negatives else "No negative values"
    return (f"{name}_range", status, message)


