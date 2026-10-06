"""Validate: apply business constraints and split rows into valid and rejected.

Every rule mirrors a constraint in sql/schema.sql, so anything that passes here is guaranteed
to load. A row can fail several rules; all reasons are recorded.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from etl.mappings import ADR_MAX, BOOKING_ID_PATTERN, REQUIRED_COLUMNS

log = logging.getLogger(__name__)

Rule = Callable[[pd.DataFrame], pd.Series]

CANCELED_STATUSES = ["Canceled", "No-Show"]

RULES: dict[str, Rule] = {
    "invalid_booking_id_format": lambda df: ~df["booking_id"].str.fullmatch(BOOKING_ID_PATTERN),
    "negative_lead_time": lambda df: df["lead_time"] < 0,
    "negative_nights": lambda df: (df["stays_in_weekend_nights"] < 0) | (df["stays_in_week_nights"] < 0),
    "negative_guest_count": lambda df: (df["adults"] < 0) | (df["children"] < 0) | (df["babies"] < 0),
    "no_guests": lambda df: (df["adults"] + df["children"] + df["babies"]) == 0,
    "adr_out_of_range": lambda df: (df["adr"] < 0) | (df["adr"] > ADR_MAX),
    "cancellation_status_mismatch": lambda df: df["is_canceled"] != df["reservation_status"].isin(CANCELED_STATUSES),
    "checkout_before_arrival": lambda df: (df["reservation_status"] == "Check-Out")
    & (df["reservation_status_date"] < df["arrival_date"]),
    "cancellation_after_arrival": lambda df: (df["reservation_status"] == "Canceled")
    & (df["reservation_status_date"] > df["arrival_date"]),
}


@dataclass
class ValidationResult:
    valid: pd.DataFrame
    rejected_reasons: pd.Series  # index = raw row position, value = list of reason codes

    @property
    def reason_counts(self) -> dict[str, int]:
        return self.rejected_reasons.explode().value_counts().to_dict()


def validate(clean: pd.DataFrame, invalid: pd.DataFrame) -> ValidationResult:
    checks: dict[str, pd.Series] = {}

    for column in clean.columns:
        if invalid[column].any():
            checks[f"invalid_{column}"] = invalid[column]
    for column in REQUIRED_COLUMNS:
        checks[f"missing_{column}"] = clean[column].isna() & ~invalid[column]
    for reason, rule in RULES.items():
        # Comparisons against missing values yield <NA>; those rows are already flagged above.
        checks[reason] = rule(clean).fillna(False).astype(bool)

    failures = pd.DataFrame(checks, index=clean.index)
    failed = failures.any(axis=1)
    reasons = failures[failed].apply(lambda row: [name for name, hit in row.items() if hit], axis=1)
    if reasons.empty:
        reasons = pd.Series(dtype=object)

    result = ValidationResult(valid=clean[~failed].copy(), rejected_reasons=reasons)
    log.info("Validation: %s valid, %s rejected", f"{len(result.valid):,}", f"{len(reasons):,}")
    for reason, count in result.reason_counts.items():
        log.info("  %-35s %6s", reason, f"{count:,}")
    return result
