-- Analytical queries over the bookings star schema.
-- Each query is tagged "-- name: <id>" so scripts/benchmark_queries.py can run them;
-- they can also be run as-is in psql or DBeaver.
-- Revenue = bookings.revenue (adr x nights, 0 for canceled bookings).

-- name: q1_top_segments_by_revenue
-- Top 10 market segments (booking categories) by revenue for arrivals in 2016 Q3.
SELECT ms.market_segment_name,
       count(*)                      AS bookings,
       sum(b.revenue)                AS revenue_eur,
       round(avg(b.revenue), 2)      AS avg_revenue_per_booking
FROM bookings b
JOIN dim_market_segment ms USING (market_segment_id)
WHERE NOT b.is_canceled
  AND b.arrival_date >= DATE '2016-07-01'
  AND b.arrival_date <  DATE '2016-10-01'
GROUP BY ms.market_segment_name
ORDER BY revenue_eur DESC
LIMIT 10;

-- name: q2_monthly_revenue_growth
-- Monthly revenue for City Hotel with month-over-month growth.
WITH monthly AS (
    SELECT date_trunc('month', b.arrival_date)::date AS month,
           sum(b.revenue)                            AS revenue_eur
    FROM bookings b
    WHERE NOT b.is_canceled
      AND b.hotel_id = (SELECT hotel_id FROM dim_hotel WHERE hotel_name = 'City Hotel')
    GROUP BY 1
)
SELECT month,
       revenue_eur,
       lag(revenue_eur) OVER (ORDER BY month) AS prev_month_eur,
       round(100.0 * (revenue_eur - lag(revenue_eur) OVER (ORDER BY month))
             / NULLIF(lag(revenue_eur) OVER (ORDER BY month), 0), 1) AS mom_growth_pct
FROM monthly
ORDER BY month;

-- name: q3_country_drilldown
-- Germany: monthly bookings, average daily rate and cancellation rate.
SELECT date_trunc('month', b.arrival_date)::date             AS month,
       count(*)                                              AS bookings,
       round(avg(b.adr), 2)                                  AS avg_daily_rate_eur,
       round(100.0 * avg(b.is_canceled::int), 1)             AS cancellation_rate_pct
FROM bookings b
WHERE b.country_code = 'DEU'
GROUP BY 1
ORDER BY 1;

-- name: q4_adr_by_country
-- Average daily rate and cancellation rate for the 10 countries with the most bookings.
-- The dataset has no guest rating, so average daily rate stands in for "average rating by
-- country". This aggregates every booking, so no index is built for it: the planner may read
-- idx_bookings_country_arrival as a narrower copy of the table, but the gain is marginal.
SELECT c.country_name,
       count(*)                                  AS bookings,
       round(avg(b.adr), 2)                      AS avg_daily_rate_eur,
       round(100.0 * avg(b.is_canceled::int), 1) AS cancellation_rate_pct
FROM bookings b
JOIN dim_country c USING (country_code)
GROUP BY c.country_name
ORDER BY bookings DESC
LIMIT 10;
