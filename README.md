# Hotel Bookings ETL Pipeline

**Associate Data Engineer Technical Examination – Luxury Explorers**

Submitted by Gimsara Elgiriyage

A small data application that ingests a messy, real-world hotel bookings export, cleans and
validates it with Python, and loads it into PostgreSQL for analysis, with AWS S3 integration.

## Project structure

```
.
├── data/
│   ├── source/        # original public dataset, unmodified
│   ├── raw/           # generated raw/dirty dataset used as the ETL input
│   └── sample/        # 200-row preview of the raw dataset
├── scripts/
│   └── make_raw_dataset.py
├── requirements.txt
└── README.md
```

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows  (source .venv/bin/activate on macOS/Linux)
pip install -r requirements.txt
```

## 1. Base dataset

### Source

[Hotel booking demand datasets](https://doi.org/10.1016/j.dib.2018.11.126) (Antonio, Almeida &
Nunes, 2019, *Data in Brief*): 119,390 real bookings for a city hotel (Lisbon) and a resort
hotel (Algarve) with arrivals between July 2015 and August 2017. The data is published under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) and was taken from the
[TidyTuesday mirror](https://github.com/rfordatascience/tidytuesday/tree/main/data/2020/2020-02-11).

Both the unmodified source (`data/source/hotels.csv`, 16.9 MB) and the generated raw file
(`data/raw/hotel_bookings_raw.csv`, 14.8 MB) are committed, so the pipeline runs on a fresh
clone without any download step.

The travel/hospitality domain was chosen because it matches the business, and the full
119k rows are kept (rather than the 10k minimum) so that the indexing benefits in PostgreSQL are
measurable rather than theoretical.

### Generating the raw dataset

```bash
python scripts/make_raw_dataset.py            # writes data/raw/hotel_bookings_raw.csv
```

The script reads `data/source/hotels.csv` (downloading it if missing), reshapes it into a single
raw export and injects seeded noise. Output is deterministic for a given `--seed` (default `42`),
so regenerating reproduces the committed file exactly.

Result: **119,390 rows x 20 columns**, 87,396 unique bookings.

| Column | Type (after cleaning) | Notes |
|---|---|---|
| `booking_id` | string | Added; identical source rows share an id |
| `hotel` | string | `City Hotel` / `Resort Hotel` |
| `is_canceled` | boolean | |
| `lead_time` | integer | Days between booking and arrival |
| `arrival_date` | date | Built from the source's separate year/month/day columns |
| `stays_in_weekend_nights`, `stays_in_week_nights` | integer | |
| `adults`, `children`, `babies` | integer | |
| `meal` | string | `BB`, `HB`, `FB`, `SC`, `Undefined` (SC and Undefined both mean no meal) |
| `country` | string | ISO 3166-1 alpha-3 |
| `market_segment`, `distribution_channel` | string | Booking category / channel |
| `deposit_type`, `customer_type` | string | |
| `adr` | numeric | Average daily rate in EUR (price) |
| `total_of_special_requests` | integer | |
| `reservation_status` | string | `Check-Out`, `Canceled`, `No-Show` |
| `reservation_status_date` | date | Date the status was last set |

### Data-quality problems in the raw file

Real problems that come with the source data:

| Problem | Rows |
|---|---|
| Duplicate bookings (same `booking_id`) | 31,994 |
| `NULL` text instead of a country | 488 |
| `NA` text instead of a children count | 4 |
| `Undefined` meal / market segment / distribution channel | 1,169 / 2 / 5 |
| China coded as `CN` instead of `CHN` | 1,279 |
| Negative ADR / ADR above 5,000 | 1 / 1 |
| Bookings with zero guests | 180 |

Noise injected by the generator to simulate a messy export (counts for seed `42`):

| Problem | Rows |
|---|---|
| `arrival_date` in 5 formats (`2015-07-01`, `01/07/2015`, `01-Jul-2015`, `2015/07/01`, `July 01, 2015`) | 35,883 non-ISO |
| `reservation_status_date` in 4 formats, some with a time component | 29,881 non-ISO |
| Impossible / unparseable arrival dates (`2016-02-30`, `TBD`, ...) | 41 |
| Inconsistent casing and whitespace in text columns (`CITY HOTEL`, `  Resort Hotel`, `online ta`) | ~46,700 |
| `is_canceled` as `yes/no`, `true/false`, `Y/N` instead of `0/1` | 9,636 |
| Country as lower-case code, padded, or full name (`prt`, ` GBR `, `Portugal`) | 8,243 |
| `adr` with currency symbols or comma decimals (`€98.00`, `98.00 EUR`, `98,00`) | 17,880 |
| Integers written as floats (`2.0`) in `adults` / `children` | 5,862 |
| Negative `lead_time` | 110 |
| Missing values as blanks or `N/A`, `null`, `-` across 7 columns | 5,022 |

Because the noise is applied per row, only 7,051 of the 31,994 duplicate copies are still
byte-identical. The rest differ only in formatting, so duplicates must be detected by key after
standardisation, not by comparing raw rows.

A 200-row preview is committed at [`data/sample/hotel_bookings_raw_sample.csv`](data/sample/hotel_bookings_raw_sample.csv).
