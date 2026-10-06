import pandas as pd
import pytest

from etl.mappings import ARRIVAL_DATE_FORMATS, RAW_COLUMNS, STATUS_DATE_FORMATS
from etl.transform import (
    deduplicate,
    resolve_country,
    standardise_boolean,
    standardise_category,
    standardise_date,
    standardise_integer,
    standardise_price,
    transform,
)
from etl.validate import validate

VALID_ROW = {
    "booking_id": "BKG-000001",
    "hotel": "City Hotel",
    "is_canceled": "0",
    "lead_time": "10",
    "arrival_date": "2016-03-01",
    "stays_in_weekend_nights": "1",
    "stays_in_week_nights": "2",
    "adults": "2",
    "children": "0",
    "babies": "0",
    "meal": "BB",
    "country": "PRT",
    "market_segment": "Online TA",
    "distribution_channel": "TA/TO",
    "deposit_type": "No Deposit",
    "customer_type": "Transient",
    "adr": "100",
    "total_of_special_requests": "0",
    "reservation_status": "Check-Out",
    "reservation_status_date": "2016-03-04",
}


def raw_frame(*overrides: dict) -> pd.DataFrame:
    return pd.DataFrame([{**VALID_ROW, **o} for o in overrides], columns=RAW_COLUMNS)


def test_mixed_arrival_date_formats_parse_to_the_same_day():
    raw = pd.Series(["2015-07-01", "01/07/2015", "01-Jul-2015", "2015/07/01", "July 01, 2015"])
    parsed, invalid = standardise_date(raw, ARRIVAL_DATE_FORMATS)
    assert (parsed == pd.Timestamp("2015-07-01")).all()
    assert not invalid.any()


def test_status_date_with_time_component_is_normalised():
    parsed, _ = standardise_date(pd.Series(["2015-07-03 00:00:00", "03.07.2015"]), STATUS_DATE_FORMATS)
    assert (parsed == pd.Timestamp("2015-07-03")).all()


@pytest.mark.parametrize("value", ["2016-02-30", "31/04/2016", "TBD"])
def test_impossible_dates_are_invalid_not_missing(value):
    parsed, invalid = standardise_date(pd.Series([value]), ARRIVAL_DATE_FORMATS)
    assert parsed.isna().all() and invalid.all()


@pytest.mark.parametrize("value", ["", "N/A", "null", "-", "  "])
def test_null_tokens_are_missing_not_invalid(value):
    parsed, invalid = standardise_date(pd.Series([value]), ARRIVAL_DATE_FORMATS)
    assert parsed.isna().all() and not invalid.any()


def test_price_formats():
    parsed, invalid = standardise_price(pd.Series(["€98.00", "98.00 EUR", "EUR 98.0", "98,00", " 98 ", "abc"]))
    assert parsed[:5].tolist() == [98.0] * 5
    assert invalid.tolist() == [False] * 5 + [True]


def test_boolean_encodings():
    parsed, _ = standardise_boolean(pd.Series(["1", "yes", "TRUE", "Y", "0", "no", "false", "n"]))
    assert parsed.tolist() == [True] * 4 + [False] * 4


def test_integers_accept_float_notation_but_reject_fractions():
    parsed, invalid = standardise_integer(pd.Series(["2", "2.0", "2.5"]))
    assert parsed[:2].tolist() == [2, 2]
    assert invalid.tolist() == [False, False, True]


def test_category_casing_whitespace_and_alias():
    hotels, _ = standardise_category(pd.Series(["CITY HOTEL", "  resort  hotel "]), "hotel")
    meals, _ = standardise_category(pd.Series(["undefined", "bb"]), "meal")
    assert hotels.tolist() == ["City Hotel", "Resort Hotel"]
    assert meals.tolist() == ["SC", "BB"]


@pytest.mark.parametrize("value,expected", [("prt", "PRT"), ("Portugal", "PRT"), ("CN", "CHN"), ("TMP", "TLS"),
                                            ("United Kingdom", "GBR"), ("Atlantis", None)])
def test_country_resolution(value, expected):
    assert resolve_country(value) == expected


def test_dedupe_keeps_the_most_complete_copy():
    typed = pd.DataFrame({"booking_id": ["BKG-1", "BKG-1", "BKG-2"], "adr": [None, 98.0, 50.0]})
    keep, drop = deduplicate(typed)
    assert keep.tolist() == [1, 2]
    assert drop.tolist() == [0]


def test_copies_heal_each_other_and_missing_defaults_apply():
    raw = raw_frame({"adr": "N/A", "country": "NULL"}, {"adr": "€100.00", "country": "NULL"})
    result = transform(raw)
    assert len(result.clean) == 1 and len(result.duplicate_index) == 1
    row = result.clean.iloc[0]
    assert row["adr"] == 100.0
    assert row["country"] == "UNK"


def test_validation_collects_every_reason():
    raw = raw_frame(
        {},
        {"booking_id": "BKG-000002", "adults": "0", "lead_time": "-5"},
        {"booking_id": "BKG-000003", "hotel": "", "arrival_date": "TBD"},
        {"booking_id": "BKG-000004", "is_canceled": "1"},
    )
    result = transform(raw)
    validated = validate(result.clean, result.invalid)
    reasons = validated.rejected_reasons
    assert validated.valid["booking_id"].tolist() == ["BKG-000001"]
    assert set(reasons[1]) == {"no_guests", "negative_lead_time"}
    assert set(reasons[2]) == {"missing_hotel", "invalid_arrival_date"}
    assert reasons[3] == ["cancellation_status_mismatch"]
