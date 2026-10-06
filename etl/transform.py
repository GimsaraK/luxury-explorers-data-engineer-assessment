"""Transform: standardise formats, remove duplicates and fill missing values.

Each standardiser returns the typed column plus an `invalid` mask that marks values which were
present in the raw file but could not be parsed (e.g. "TBD" as a date). Missing and invalid are
kept apart because they are handled differently: missing values may be filled with a default,
invalid values are always rejected.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import lru_cache

import pandas as pd
import pycountry

from etl.mappings import (
    ARRIVAL_DATE_FORMATS,
    BOOLEAN_VALUES,
    CATEGORY_ALIASES,
    CATEGORY_VALUES,
    COUNTRY_OVERRIDES,
    INTEGER_COLUMNS,
    MISSING_VALUE_DEFAULTS,
    NULL_TOKENS,
    STATUS_DATE_FORMATS,
)

log = logging.getLogger(__name__)


@dataclass
class TransformResult:
    clean: pd.DataFrame          # typed, deduplicated, defaults applied; index = raw row position
    invalid: pd.DataFrame        # bool, same shape as `clean`: present in raw but unparseable
    duplicate_index: pd.Index    # raw rows dropped as duplicates
    filled_counts: dict[str, int] = field(default_factory=dict)


def normalise_text(s: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace and turn null tokens into <NA>."""
    s = s.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return s.mask(s.str.lower().isin(NULL_TOKENS))


def standardise_category(s: pd.Series, column: str) -> tuple[pd.Series, pd.Series]:
    lookup = {value.lower(): value for value in CATEGORY_VALUES[column]}
    text = normalise_text(s)
    mapped = text.str.lower().map(lookup).astype("string")
    aliases = CATEGORY_ALIASES.get(column)
    if aliases:
        mapped = mapped.replace(aliases)
    return mapped, text.notna() & mapped.isna()


def standardise_boolean(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    text = normalise_text(s)
    mapped = text.str.lower().map(BOOLEAN_VALUES).astype("boolean")
    return mapped, text.notna() & mapped.isna()


def standardise_integer(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    text = normalise_text(s)
    number = pd.to_numeric(text, errors="coerce")
    whole = number.notna() & (number % 1 == 0)
    result = number.where(whole).astype("Int64")
    return result, text.notna() & result.isna()


def standardise_price(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse '€98.00', '98.00 EUR', 'EUR 98.0' and '98,00' into 98.0."""
    text = normalise_text(s)
    stripped = text.str.replace("€", "", regex=False).str.replace("EUR", "", case=False, regex=False).str.strip()
    comma_decimal = stripped.str.contains(",", regex=False) & ~stripped.str.contains(".", regex=False)
    stripped = stripped.mask(comma_decimal.fillna(False), stripped.str.replace(",", ".", regex=False))
    number = pd.to_numeric(stripped, errors="coerce").round(2)
    return number, text.notna() & number.isna()


def standardise_date(s: pd.Series, formats: list[str]) -> tuple[pd.Series, pd.Series]:
    """Try each known format in turn; impossible dates such as 2016-02-30 stay NaT."""
    text = normalise_text(s)
    parsed = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in formats:
        todo = parsed.isna() & text.notna()
        if not todo.any():
            break
        parsed[todo] = pd.to_datetime(text[todo], format=fmt, errors="coerce")
    parsed = parsed.dt.normalize()
    return parsed, text.notna() & parsed.isna()


@lru_cache(maxsize=None)
def resolve_country(value: str) -> str | None:
    """Map an ISO alpha-2/alpha-3 code or a country name to an ISO alpha-3 code."""
    upper = value.upper()
    if upper in COUNTRY_OVERRIDES:
        return COUNTRY_OVERRIDES[upper]
    if len(upper) == 3 and pycountry.countries.get(alpha_3=upper):
        return upper
    try:
        return pycountry.countries.lookup(value).alpha_3
    except LookupError:
        return None


def standardise_country(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    text = normalise_text(s)
    mapping = {value: resolve_country(value) for value in text.dropna().unique()}
    mapped = text.map(mapping).astype("string")
    return mapped, text.notna() & mapped.isna()


def standardise(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    typed: dict[str, pd.Series] = {}
    invalid: dict[str, pd.Series] = {}

    def put(column: str, result: tuple[pd.Series, pd.Series]) -> None:
        typed[column], invalid[column] = result

    booking_id = normalise_text(raw["booking_id"]).str.upper()
    put("booking_id", (booking_id, pd.Series(False, index=raw.index)))
    for column in CATEGORY_VALUES:
        put(column, standardise_category(raw[column], column))
    for column in INTEGER_COLUMNS:
        put(column, standardise_integer(raw[column]))
    put("is_canceled", standardise_boolean(raw["is_canceled"]))
    put("adr", standardise_price(raw["adr"]))
    put("country", standardise_country(raw["country"]))
    put("arrival_date", standardise_date(raw["arrival_date"], ARRIVAL_DATE_FORMATS))
    put("reservation_status_date", standardise_date(raw["reservation_status_date"], STATUS_DATE_FORMATS))

    columns = list(raw.columns)
    return pd.DataFrame(typed)[columns], pd.DataFrame(invalid)[columns]


def deduplicate(typed: pd.DataFrame) -> tuple[pd.Index, pd.Index]:
    """Keep the most complete copy of each booking_id; ties keep the earliest row.

    Copies only differ by formatting noise and by which fields were blanked, so after
    standardisation the most complete copy is the best surviving version of the booking.
    Rows without a booking_id cannot be matched and are left for validation to reject.
    """
    completeness = typed.notna().sum(axis=1)
    ordered = completeness.sort_values(ascending=False, kind="stable").index
    ids = typed.loc[ordered, "booking_id"]
    is_copy = ids.duplicated() & ids.notna()
    keep = ordered[~is_copy.to_numpy()].sort_values()
    drop = ordered[is_copy.to_numpy()].sort_values()
    return keep, drop


def fill_missing(clean: pd.DataFrame, invalid: pd.DataFrame) -> dict[str, int]:
    filled = {}
    for column, default in MISSING_VALUE_DEFAULTS.items():
        mask = clean[column].isna() & ~invalid[column]
        clean.loc[mask, column] = default
        filled[column] = int(mask.sum())
    return filled


def transform(raw: pd.DataFrame) -> TransformResult:
    typed, invalid = standardise(raw)
    log.info("Standardised %s rows; unparseable values per column: %s",
             f"{len(typed):,}", {k: int(v) for k, v in invalid.sum().items() if v})

    keep, drop = deduplicate(typed)
    clean = typed.loc[keep].copy()
    invalid = invalid.loc[keep]
    log.info("Removed %s duplicate rows; %s unique bookings remain", f"{len(drop):,}", f"{len(clean):,}")

    filled = fill_missing(clean, invalid)
    log.info("Filled missing values with defaults: %s", {k: v for k, v in filled.items() if v})
    return TransformResult(clean=clean, invalid=invalid, duplicate_index=drop, filled_counts=filled)
