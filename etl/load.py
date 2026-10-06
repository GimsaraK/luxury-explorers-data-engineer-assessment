"""Load: write dimensions and the bookings fact table to PostgreSQL, plus ETL audit records.

The fact load is set-based: clean rows are streamed into a temporary staging table with COPY,
then merged into `bookings` with INSERT ... ON CONFLICT, so re-running the pipeline updates
rows in place instead of duplicating them. The whole load runs in a single transaction.
"""

from __future__ import annotations

import io
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import pycountry
from psycopg2 import sql
from psycopg2.extensions import connection as Connection

from etl.mappings import HOTEL_ATTRIBUTES, UNKNOWN_COUNTRY_CODE, UNKNOWN_COUNTRY_NAME

log = logging.getLogger(__name__)

# (table, surrogate key, name column, source column in the clean frame)
LOOKUP_DIMENSIONS = [
    ("dim_market_segment", "market_segment_id", "market_segment_name", "market_segment"),
    ("dim_distribution_channel", "distribution_channel_id", "distribution_channel_name", "distribution_channel"),
    ("dim_customer_type", "customer_type_id", "customer_type_name", "customer_type"),
]

FACT_COLUMNS = [
    "booking_id",
    "hotel_id",
    "country_code",
    "market_segment_id",
    "distribution_channel_id",
    "customer_type_id",
    "is_canceled",
    "lead_time",
    "arrival_date",
    "stays_in_weekend_nights",
    "stays_in_week_nights",
    "adults",
    "children",
    "babies",
    "meal",
    "deposit_type",
    "adr",
    "total_of_special_requests",
    "reservation_status",
    "reservation_status_date",
    "etl_run_id",
]

STAGING_DDL = """
CREATE TEMP TABLE stg_bookings (
    booking_id                 VARCHAR(10),
    hotel_id                   SMALLINT,
    country_code               CHAR(3),
    market_segment_id          SMALLINT,
    distribution_channel_id    SMALLINT,
    customer_type_id           SMALLINT,
    is_canceled                BOOLEAN,
    lead_time                  SMALLINT,
    arrival_date               DATE,
    stays_in_weekend_nights    SMALLINT,
    stays_in_week_nights       SMALLINT,
    adults                     SMALLINT,
    children                   SMALLINT,
    babies                     SMALLINT,
    meal                       VARCHAR(2),
    deposit_type               VARCHAR(12),
    adr                        NUMERIC(10, 2),
    total_of_special_requests  SMALLINT,
    reservation_status         VARCHAR(10),
    reservation_status_date    DATE,
    etl_run_id                 UUID
) ON COMMIT DROP
"""


def connect(connect_kwargs: dict[str, object]) -> Connection:
    conn = psycopg2.connect(**connect_kwargs, application_name="hotel_bookings_etl")
    log.info("Connected to PostgreSQL %s:%s/%s", connect_kwargs["host"], connect_kwargs["port"], connect_kwargs["dbname"])
    return conn


def apply_schema(conn: Connection, *sql_paths: Path) -> None:
    with conn, conn.cursor() as cur:
        for path in sql_paths:
            cur.execute(path.read_text(encoding="utf-8"))
            log.info("Applied %s", path.name)


def start_run(
    conn: Connection,
    run_id: uuid.UUID,
    source_file: str,
    sha256: str,
    raw_s3_uri: str | None = None,
    processed_s3_uri: str | None = None,
) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl.pipeline_runs
                (run_id, started_at, status, source_file, source_sha256, raw_s3_uri, processed_s3_uri)
            VALUES (%s, %s, 'running', %s, %s, %s, %s)
            """,
            (str(run_id), datetime.now(timezone.utc), source_file, sha256, raw_s3_uri, processed_s3_uri),
        )


def finish_run(
    conn: Connection,
    run_id: uuid.UUID,
    status: str,
    counts: dict[str, int],
    error: str | None = None,
) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl.pipeline_runs
               SET finished_at = %s, status = %s, rows_extracted = %s, rows_duplicates = %s,
                   rows_rejected = %s, rows_loaded = %s, error_message = %s
             WHERE run_id = %s
            """,
            (
                datetime.now(timezone.utc),
                status,
                counts.get("extracted"),
                counts.get("duplicates"),
                counts.get("rejected"),
                counts.get("loaded"),
                error,
                str(run_id),
            ),
        )


def _upsert_hotels(cur, hotels: list[str]) -> dict[str, int]:
    rows = [(name, HOTEL_ATTRIBUTES[name]["hotel_type"], HOTEL_ATTRIBUTES[name]["location"]) for name in hotels]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO dim_hotel (hotel_name, hotel_type, location) VALUES %s ON CONFLICT (hotel_name) DO NOTHING",
        rows,
    )
    cur.execute("SELECT hotel_name, hotel_id FROM dim_hotel")
    return dict(cur.fetchall())


def _upsert_countries(cur, codes: list[str]) -> None:
    def name(code: str) -> str:
        if code == UNKNOWN_COUNTRY_CODE:
            return UNKNOWN_COUNTRY_NAME
        country = pycountry.countries.get(alpha_3=code)
        return country.name if country else code

    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO dim_country (country_code, country_name) VALUES %s ON CONFLICT (country_code) DO NOTHING",
        [(code, name(code)) for code in codes],
    )


def _upsert_lookup(cur, table: str, key: str, name_column: str, values: list[str]) -> dict[str, int]:
    cur.execute(
        sql.SQL("INSERT INTO {t} ({n}) SELECT unnest(%s::text[]) ON CONFLICT ({n}) DO NOTHING").format(
            t=sql.Identifier(table), n=sql.Identifier(name_column)
        ),
        (values,),
    )
    cur.execute(
        sql.SQL("SELECT {n}, {k} FROM {t}").format(
            t=sql.Identifier(table), n=sql.Identifier(name_column), k=sql.Identifier(key)
        )
    )
    return dict(cur.fetchall())


def _to_fact_frame(clean: pd.DataFrame, cur, run_id: uuid.UUID) -> pd.DataFrame:
    fact = clean.copy()
    hotel_ids = _upsert_hotels(cur, sorted(fact["hotel"].unique()))
    fact["hotel_id"] = fact["hotel"].map(hotel_ids)

    _upsert_countries(cur, sorted(fact["country"].unique()))
    fact["country_code"] = fact["country"]

    for table, key, name_column, source in LOOKUP_DIMENSIONS:
        ids = _upsert_lookup(cur, table, key, name_column, sorted(fact[source].unique()))
        fact[key] = fact[source].map(ids)

    for column in ("arrival_date", "reservation_status_date"):
        fact[column] = fact[column].dt.strftime("%Y-%m-%d")
    fact["etl_run_id"] = str(run_id)
    return fact[FACT_COLUMNS]


def load_bookings(conn: Connection, run_id: uuid.UUID, clean: pd.DataFrame, rejected: pd.DataFrame) -> int:
    """Load dimensions, the fact table and rejected records atomically. Returns rows upserted."""
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in FACT_COLUMNS if c != "booking_id"
    )
    columns = sql.SQL(", ").join(map(sql.Identifier, FACT_COLUMNS))
    merge = sql.SQL(
        "INSERT INTO bookings ({cols}) SELECT {cols} FROM stg_bookings "
        "ON CONFLICT (booking_id) DO UPDATE SET {updates}, loaded_at = now()"
    ).format(cols=columns, updates=updates)

    with conn, conn.cursor() as cur:
        fact = _to_fact_frame(clean, cur, run_id)

        cur.execute(STAGING_DDL)
        buffer = io.StringIO()
        fact.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cur.copy_expert(
            sql.SQL("COPY stg_bookings ({cols}) FROM STDIN WITH (FORMAT csv)").format(cols=columns),
            buffer,
        )
        log.info("Staged %s rows with COPY", f"{len(fact):,}")

        cur.execute(merge)
        loaded = cur.rowcount
        log.info("Upserted %s rows into bookings", f"{loaded:,}")

        _save_rejections(cur, run_id, rejected)
    return loaded


def vacuum_analyze(conn: Connection) -> None:
    """Reclaim the row versions replaced by the upsert and refresh planner statistics.

    Index Only Scans can skip the table only for pages marked all-visible, which VACUUM sets.
    """
    conn.autocommit = True  # VACUUM cannot run inside a transaction block
    try:
        with conn.cursor() as cur:
            cur.execute("VACUUM (ANALYZE) bookings")
    finally:
        conn.autocommit = False
    log.info("VACUUM ANALYZE bookings completed")


def _save_rejections(cur, run_id: uuid.UUID, rejected: pd.DataFrame) -> None:
    if rejected.empty:
        return
    raw_columns = [c for c in rejected.columns if c not in ("source_line", "rejection_reasons")]
    rows = [
        (
            str(run_id),
            int(row.source_line),
            row.booking_id or None,
            row.rejection_reasons.split(";"),
            json.dumps({c: getattr(row, c) for c in raw_columns}, ensure_ascii=False),
        )
        for row in rejected.itertuples(index=False)
    ]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO etl.rejected_records (run_id, source_line, booking_id, reasons, raw_record) VALUES %s",
        rows,
        template="(%s, %s, %s, %s, %s::jsonb)",
        page_size=1000,
    )
    log.info("Recorded %s rejected rows in etl.rejected_records", f"{len(rows):,}")
