-- Hotel bookings star schema + ETL audit tables.
-- Idempotent: safe to run on every pipeline execution.
-- Secondary (FK / analytical) indexes live in sql/indexes.sql.

CREATE SCHEMA IF NOT EXISTS etl;

-- ---------------------------------------------------------------------------
-- ETL audit
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id           UUID         PRIMARY KEY,
    started_at       TIMESTAMPTZ  NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(10)  NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    source_file      TEXT         NOT NULL,
    source_sha256    CHAR(64)     NOT NULL,
    rows_extracted   INTEGER      CHECK (rows_extracted >= 0),
    rows_duplicates  INTEGER      CHECK (rows_duplicates >= 0),
    rows_rejected    INTEGER      CHECK (rows_rejected >= 0),
    rows_loaded      INTEGER      CHECK (rows_loaded >= 0),
    error_message    TEXT
);

CREATE TABLE IF NOT EXISTS etl.rejected_records (
    rejected_id  BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       UUID         NOT NULL REFERENCES etl.pipeline_runs (run_id) ON DELETE CASCADE,
    source_line  INTEGER      NOT NULL,
    booking_id   VARCHAR(50),
    reasons      TEXT[]       NOT NULL CHECK (cardinality(reasons) > 0),
    raw_record   JSONB        NOT NULL,
    rejected_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_hotel (
    hotel_id    SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hotel_name  VARCHAR(50)  NOT NULL UNIQUE,
    hotel_type  VARCHAR(10)  NOT NULL CHECK (hotel_type IN ('City', 'Resort')),
    location    VARCHAR(50)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_code  CHAR(3)       PRIMARY KEY CHECK (country_code ~ '^[A-Z]{3}$'),
    country_name  VARCHAR(100)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_market_segment (
    market_segment_id    SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_segment_name  VARCHAR(30)  NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_distribution_channel (
    distribution_channel_id    SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    distribution_channel_name  VARCHAR(30)  NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_customer_type (
    customer_type_id    SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_type_name  VARCHAR(30)  NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------
-- Fact
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bookings (
    booking_id                 VARCHAR(10)    PRIMARY KEY CHECK (booking_id ~ '^BKG-[0-9]{6}$'),
    hotel_id                   SMALLINT       NOT NULL REFERENCES dim_hotel (hotel_id),
    country_code               CHAR(3)        NOT NULL REFERENCES dim_country (country_code),
    market_segment_id          SMALLINT       NOT NULL REFERENCES dim_market_segment (market_segment_id),
    distribution_channel_id    SMALLINT       NOT NULL REFERENCES dim_distribution_channel (distribution_channel_id),
    customer_type_id           SMALLINT       NOT NULL REFERENCES dim_customer_type (customer_type_id),
    is_canceled                BOOLEAN        NOT NULL,
    lead_time                  SMALLINT       NOT NULL CHECK (lead_time >= 0),
    arrival_date               DATE           NOT NULL,
    stays_in_weekend_nights    SMALLINT       NOT NULL CHECK (stays_in_weekend_nights >= 0),
    stays_in_week_nights       SMALLINT       NOT NULL CHECK (stays_in_week_nights >= 0),
    total_nights               INTEGER        GENERATED ALWAYS AS (stays_in_weekend_nights + stays_in_week_nights) STORED,
    adults                     SMALLINT       NOT NULL CHECK (adults >= 0),
    children                   SMALLINT       NOT NULL CHECK (children >= 0),
    babies                     SMALLINT       NOT NULL CHECK (babies >= 0),
    meal                       VARCHAR(2)     NOT NULL CHECK (meal IN ('BB', 'HB', 'FB', 'SC')),
    deposit_type               VARCHAR(12)    NOT NULL CHECK (deposit_type IN ('No Deposit', 'Non Refund', 'Refundable')),
    adr                        NUMERIC(10, 2) NOT NULL CHECK (adr BETWEEN 0 AND 1000),
    total_of_special_requests  SMALLINT       NOT NULL CHECK (total_of_special_requests >= 0),
    reservation_status         VARCHAR(10)    NOT NULL CHECK (reservation_status IN ('Check-Out', 'Canceled', 'No-Show')),
    reservation_status_date    DATE           NOT NULL,
    revenue                    NUMERIC(12, 2) GENERATED ALWAYS AS (
                                   CASE WHEN is_canceled THEN 0
                                        ELSE adr * (stays_in_weekend_nights + stays_in_week_nights)
                                   END
                               ) STORED,
    etl_run_id                 UUID           NOT NULL REFERENCES etl.pipeline_runs (run_id),
    loaded_at                  TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT chk_bookings_has_guests
        CHECK (adults + children + babies > 0),
    CONSTRAINT chk_bookings_cancellation_matches_status
        CHECK (is_canceled = (reservation_status IN ('Canceled', 'No-Show'))),
    CONSTRAINT chk_bookings_checkout_not_before_arrival
        CHECK (reservation_status <> 'Check-Out' OR reservation_status_date >= arrival_date),
    CONSTRAINT chk_bookings_cancellation_not_after_arrival
        CHECK (reservation_status <> 'Canceled' OR reservation_status_date <= arrival_date)
);
