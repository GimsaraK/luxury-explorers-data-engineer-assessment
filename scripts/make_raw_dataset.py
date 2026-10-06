"""Build the raw, intentionally messy hotel bookings dataset used as the ETL source.

Source: "Hotel booking demand datasets" (Antonio, Almeida & Nunes, 2019, Data in Brief),
119,390 real bookings for a city hotel and a resort hotel in Portugal (Jul 2015 - Aug 2017),
as republished by TidyTuesday (2020-02-11).

The source already contains real data-quality problems that are kept as-is:
  * 31,994 rows that are exact copies of another row (re-exported bookings)
  * "NULL" / "NA" text tokens instead of empty values (country, children)
  * "Undefined" categories (meal, market_segment, distribution_channel)
  * mixed country code standards (ISO-3 everywhere, but "CN" for China)
  * invalid values: negative / extreme ADR, bookings with zero guests

On top of that, this script simulates a raw export from a property-management system by:
  * adding a `booking_id` (identical source rows share the same id, so the duplicates are
    detectable by key and still differ in formatting after the noise below is applied)
  * collapsing the split arrival year/month/day columns into one `arrival_date` string
  * injecting seeded formatting noise: mixed date formats, casing/whitespace variants,
    currency-formatted prices, mixed boolean encodings and mixed missing-value tokens
  * injecting a small number of invalid values (impossible dates, negative lead times)

Usage:
    python scripts/make_raw_dataset.py [--seed 42] [--output data/raw/hotel_bookings_raw.csv]
"""

from __future__ import annotations

import argparse
import logging
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

SOURCE_URL = (
    "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/"
    "data/2020/2020-02-11/hotels.csv"
)
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data" / "source" / "hotels.csv"
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "hotel_bookings_raw.csv"
DEFAULT_SAMPLE = ROOT / "data" / "sample" / "hotel_bookings_raw_sample.csv"

OUTPUT_COLUMNS = [
    "booking_id",
    "hotel",
    "is_canceled",
    "lead_time",
    "arrival_date",
    "stays_in_weekend_nights",
    "stays_in_week_nights",
    "adults",
    "children",
    "babies",
    "meal",
    "country",
    "market_segment",
    "distribution_channel",
    "deposit_type",
    "customer_type",
    "adr",
    "total_of_special_requests",
    "reservation_status",
    "reservation_status_date",
]

# Tokens that already mean "missing" in the source; noise is never applied on top of them.
SOURCE_NULL_TOKENS = {"NULL", "NA", ""}
INJECTED_NULL_TOKENS = ["", "N/A", "null", "-", "  "]

ARRIVAL_DATE_FORMATS = {  # format -> share of rows
    "%Y-%m-%d": 0.70,
    "%d/%m/%Y": 0.12,
    "%d-%b-%Y": 0.08,
    "%Y/%m/%d": 0.05,
    "%B %d, %Y": 0.05,
}
STATUS_DATE_FORMATS = {
    "%Y-%m-%d": 0.75,
    "%Y-%m-%d %H:%M:%S": 0.10,
    "%d/%m/%Y": 0.10,
    "%d.%m.%Y": 0.05,
}
IMPOSSIBLE_DATES = ["2016-02-30", "31/04/2016", "2017-13-01", "TBD", "0000-00-00"]

COUNTRY_NAMES = {
    "PRT": "Portugal",
    "GBR": "United Kingdom",
    "FRA": "France",
    "ESP": "Spain",
    "DEU": "Germany",
    "ITA": "Italy",
    "IRL": "Ireland",
    "BEL": "Belgium",
    "BRA": "Brazil",
    "NLD": "Netherlands",
    "USA": "United States",
    "CHE": "Switzerland",
}

log = logging.getLogger("make_raw_dataset")


def download_source(path: Path) -> None:
    if path.exists():
        log.info("Using cached source file %s", path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading source dataset from %s", SOURCE_URL)
    urllib.request.urlretrieve(SOURCE_URL, path)


class NoiseInjector:
    """Applies seeded, per-cell noise and keeps a tally of what was changed."""

    def __init__(self, df: pd.DataFrame, seed: int) -> None:
        self.df = df
        self.rng = np.random.default_rng(seed)
        self.report: dict[str, int] = {}

    def _mask(self, column: str, rate: float, eligible: pd.Series | None = None) -> pd.Series:
        present = ~self.df[column].isin(SOURCE_NULL_TOKENS)
        if eligible is not None:
            present &= eligible
        return present & (self.rng.random(len(self.df)) < rate)

    def _tally(self, label: str, mask: pd.Series) -> None:
        self.report[label] = self.report.get(label, 0) + int(mask.sum())

    def casing_and_whitespace(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate)
        variants = [
            lambda s: s.str.lower(),
            lambda s: s.str.upper(),
            lambda s: s + " ",
            lambda s: "  " + s,
            lambda s: s.str.replace(" ", "  ", regex=False),
        ]
        choice = self.rng.integers(0, len(variants), len(self.df))
        for i, variant in enumerate(variants):
            sub = mask & (choice == i)
            self.df.loc[sub, column] = variant(self.df.loc[sub, column])
        self._tally(f"{column}: casing/whitespace variants", mask)

    def mixed_date_formats(self, column: str, formats: dict[str, float]) -> None:
        parsed = pd.to_datetime(self.df[column], format="%Y-%m-%d")
        fmt_list = list(formats)
        choice = self.rng.choice(len(fmt_list), size=len(self.df), p=list(formats.values()))
        out = self.df[column].copy()
        for i, fmt in enumerate(fmt_list):
            sub = choice == i
            out[sub] = parsed[sub].dt.strftime(fmt)
            if fmt != "%Y-%m-%d":
                self.report[f"{column}: formatted as '{fmt}'"] = int(sub.sum())
        self.df[column] = out

    def impossible_dates(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate)
        self.df.loc[mask, column] = self.rng.choice(IMPOSSIBLE_DATES, size=int(mask.sum()))
        self._tally(f"{column}: impossible/unparseable dates", mask)

    def boolean_encodings(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate)
        encodings = [("no", "yes"), ("false", "true"), ("N", "Y")]
        choice = self.rng.integers(0, len(encodings), len(self.df))
        for i, (false_tok, true_tok) in enumerate(encodings):
            sub = mask & (choice == i)
            self.df.loc[sub, column] = np.where(self.df.loc[sub, column] == "1", true_tok, false_tok)
        self._tally(f"{column}: yes/no, true/false, Y/N encodings", mask)

    def country_variants(self, rate: float) -> None:
        col = "country"
        lower = self._mask(col, rate / 2)
        self.df.loc[lower, col] = self.df.loc[lower, col].str.lower()
        self._tally("country: lower-case codes", lower)

        padded = self._mask(col, rate / 4) & ~lower
        self.df.loc[padded, col] = " " + self.df.loc[padded, col] + " "
        self._tally("country: padded with whitespace", padded)

        named = self._mask(col, rate / 2, eligible=self.df[col].isin(COUNTRY_NAMES)) & ~lower & ~padded
        self.df.loc[named, col] = self.df.loc[named, col].map(COUNTRY_NAMES)
        self._tally("country: full name instead of ISO code", named)

    def currency_formatting(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate)
        values = self.df.loc[mask, column].astype(float)
        styles = [
            lambda v: "€" + v.map("{:.2f}".format),
            lambda v: v.map("{:.2f} EUR".format),
            lambda v: v.map("{:.2f}".format).str.replace(".", ",", regex=False),
            lambda v: "EUR " + v.map("{:.1f}".format),
        ]
        choice = self.rng.integers(0, len(styles), int(mask.sum()))
        formatted = pd.Series(index=values.index, dtype=object)
        for i, style in enumerate(styles):
            idx = values.index[choice == i]
            formatted[idx] = style(values[idx])
        self.df.loc[mask, column] = formatted
        self._tally(f"{column}: currency symbols / comma decimals", mask)

    def float_like_integers(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate)
        self.df.loc[mask, column] = self.df.loc[mask, column] + ".0"
        self._tally(f"{column}: integers written as floats", mask)

    def negative_numbers(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate, eligible=self.df[column] != "0")
        self.df.loc[mask, column] = "-" + self.df.loc[mask, column]
        self._tally(f"{column}: negative (invalid) values", mask)

    def missing_values(self, column: str, rate: float) -> None:
        mask = self._mask(column, rate)
        self.df.loc[mask, column] = self.rng.choice(INJECTED_NULL_TOKENS, size=int(mask.sum()))
        self._tally(f"{column}: injected missing values", mask)


def build_raw_dataset(source: pd.DataFrame, seed: int) -> tuple[pd.DataFrame, dict[str, int]]:
    df = source.copy()

    # Identical source rows are the same booking exported more than once, so they share an id.
    group = df.groupby(list(source.columns), sort=False).ngroup()
    df["booking_id"] = "BKG-" + (group + 1).astype(str).str.zfill(6)

    df["arrival_date"] = pd.to_datetime(
        df["arrival_date_year"] + "-" + df["arrival_date_month"] + "-" + df["arrival_date_day_of_month"],
        format="%Y-%B-%d",
    ).dt.strftime("%Y-%m-%d")
    df = df[OUTPUT_COLUMNS].copy()

    noise = NoiseInjector(df, seed)
    noise.mixed_date_formats("arrival_date", ARRIVAL_DATE_FORMATS)
    noise.mixed_date_formats("reservation_status_date", STATUS_DATE_FORMATS)
    noise.impossible_dates("arrival_date", 0.0005)
    for column, rate in [
        ("hotel", 0.12),
        ("meal", 0.05),
        ("market_segment", 0.05),
        ("distribution_channel", 0.04),
        ("deposit_type", 0.04),
        ("customer_type", 0.05),
        ("reservation_status", 0.04),
    ]:
        noise.casing_and_whitespace(column, rate)
    noise.boolean_encodings("is_canceled", 0.08)
    noise.country_variants(0.06)
    noise.currency_formatting("adr", 0.15)
    noise.float_like_integers("children", 0.03)
    noise.float_like_integers("adults", 0.02)
    noise.negative_numbers("lead_time", 0.001)
    for column, rate in [
        ("hotel", 0.003),
        ("arrival_date", 0.004),
        ("meal", 0.01),
        ("market_segment", 0.008),
        ("customer_type", 0.008),
        ("adr", 0.006),
        ("adults", 0.002),
    ]:
        noise.missing_values(column, rate)

    # A raw export has no meaningful order; shuffling also scatters the duplicate copies.
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)
    return df, noise.report


def summarise(df: pd.DataFrame, report: dict[str, int]) -> None:
    dup_by_key = df["booking_id"].duplicated().sum()
    exact_dups = df.duplicated().sum()
    log.info("Rows: %s | columns: %s", f"{len(df):,}", df.shape[1])
    log.info("Unique booking_ids: %s", f"{df['booking_id'].nunique():,}")
    log.info("Duplicate rows by booking_id: %s (of which still byte-identical: %s)",
             f"{dup_by_key:,}", f"{exact_dups:,}")
    missing_tokens = SOURCE_NULL_TOKENS | {t.strip() for t in INJECTED_NULL_TOKENS} | {"N/A", "null", "-"}
    stripped = df.apply(lambda s: s.str.strip())
    missing = stripped.isin(missing_tokens).sum()
    log.info("Missing-value tokens per column: %s", {k: int(v) for k, v in missing[missing > 0].items()})
    log.info("Injected noise:")
    for label, count in report.items():
        log.info("  %-55s %8s", label, f"{count:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--sample-rows", type=int, default=200)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    download_source(args.source)
    source = pd.read_csv(args.source, dtype=str, keep_default_na=False)
    log.info("Loaded source: %s rows x %s columns", f"{len(source):,}", source.shape[1])

    raw, report = build_raw_dataset(source, args.seed)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    raw.to_csv(args.output, index=False, encoding="utf-8", lineterminator="\n")
    log.info("Wrote raw dataset to %s", args.output)

    args.sample.parent.mkdir(parents=True, exist_ok=True)
    raw.head(args.sample_rows).to_csv(args.sample, index=False, encoding="utf-8", lineterminator="\n")
    log.info("Wrote %s-row preview to %s", args.sample_rows, args.sample)

    summarise(raw, report)


if __name__ == "__main__":
    main()
