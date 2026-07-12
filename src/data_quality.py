"""
Data quality gate, run after sentiment scoring and before it's considered
ready for dbt to build marts on top of.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(Exception):
    pass


VALID_SENTIMENT_LABELS = {"POSITIVE", "NEGATIVE", None}


def _check_row_count(df: pd.DataFrame, minimum: int = 1000) -> CheckResult:
    return CheckResult("row_count_minimum", len(df) >= minimum, f"expected >= {minimum}, got {len(df)}")


def _check_sentiment_labels_valid(df: pd.DataFrame) -> CheckResult:
    invalid = df[~df["sentiment_label"].isin(VALID_SENTIMENT_LABELS)]
    return CheckResult(
        "sentiment_labels_valid", len(invalid) == 0, f"{len(invalid)} rows with an unexpected sentiment label"
    )


def _check_sentiment_score_range(df: pd.DataFrame) -> CheckResult:
    scored = df[df["sentiment_score"].notna()]
    out_of_range = scored[~scored["sentiment_score"].between(0, 1)]
    return CheckResult(
        "sentiment_score_in_range", len(out_of_range) == 0, f"{len(out_of_range)} rows with a score outside [0,1]"
    )


def _check_recommendation_values_valid(df: pd.DataFrame) -> CheckResult:
    valid = {"Recommended", "Not Recommended"}
    invalid = df[~df["recommendation"].isin(valid)]
    return CheckResult(
        "recommendation_values_valid", len(invalid) == 0, f"{len(invalid)} rows with an unexpected recommendation value"
    )


CHECKS = [
    _check_row_count,
    _check_sentiment_labels_valid,
    _check_sentiment_score_range,
    _check_recommendation_values_valid,
]


def run_data_quality_checks(df: pd.DataFrame) -> list[CheckResult]:
    results = [check(df) for check in CHECKS]
    for r in results:
        level = logging.INFO if r.passed else logging.ERROR
        logger.log(level, "[%s] %s - %s", "PASS" if r.passed else "FAIL", r.name, r.detail)

    failed = [r for r in results if not r.passed]
    if failed:
        raise DataQualityError(f"Failed checks: {', '.join(r.name for r in failed)}")
    return results
