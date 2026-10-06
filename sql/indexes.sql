-- Secondary indexes, each shaped for a query in sql/analytical_queries.sql.
-- Idempotent: applied by the pipeline after sql/schema.sql.
--
-- Partial (WHERE NOT is_canceled): only non-canceled bookings carry revenue, so revenue
-- queries never need the canceled ~28% of rows and the index stays smaller.
-- Covering (INCLUDE): every column the query reads is in the index, which allows an
-- Index Only Scan that never touches the 24 MB table.

-- q1_top_segments_by_revenue: filter on an arrival date range, group by segment, sum revenue.
CREATE INDEX IF NOT EXISTS idx_bookings_arrival_segment_revenue
    ON bookings (arrival_date)
    INCLUDE (market_segment_id, revenue)
    WHERE NOT is_canceled;

-- q2_monthly_revenue_growth: equality on hotel first, then the range/sort column.
CREATE INDEX IF NOT EXISTS idx_bookings_hotel_arrival_revenue
    ON bookings (hotel_id, arrival_date)
    INCLUDE (revenue)
    WHERE NOT is_canceled;

-- q3_country_drilldown: equality on country first, then arrival month. Not partial, because
-- the cancellation rate needs canceled bookings too.
CREATE INDEX IF NOT EXISTS idx_bookings_country_arrival
    ON bookings (country_code, arrival_date)
    INCLUDE (adr, is_canceled);

-- Foreign key used to filter rejected records by run; PostgreSQL does not index FKs itself.
CREATE INDEX IF NOT EXISTS idx_rejected_records_run_id
    ON etl.rejected_records (run_id);
