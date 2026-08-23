-- Uber Rides Analytics - AI/BI dashboard datasets
-- Source of truth: dashboard/uber_rides_analytics.lvdash.json
--
-- Catalog/schema are supplied by the dashboard at runtime
-- (--dataset-catalog uber_project --dataset-schema gold), so the
-- FROM clauses below intentionally use bare table names.
--
-- :start_date / :end_date are dashboard DATE parameters, bound to the
-- date-range filter on every page.

-- ========================================================================
-- rides_detail  (Rides detail)
-- parameters: start_date, end_date
-- ========================================================================
WITH driver_current AS (
    SELECT driver_id, driver_name, driver_rating
    FROM dim_driver WHERE __END_AT IS NULL
),
passenger_current AS (
    SELECT passenger_id, passenger_name
    FROM dim_passenger WHERE __END_AT IS NULL
),
vehicle_current AS (
    SELECT vehicle_id, vehicle_model, vehicle_color
    FROM dim_vehicle WHERE __END_AT IS NULL
),
driver_revenue AS (
    SELECT driver_id,
           SUM(CASE WHEN is_completed THEN total_fare END) AS driver_total_revenue,
           ROW_NUMBER() OVER (
               ORDER BY SUM(CASE WHEN is_completed THEN total_fare END) DESC NULLS LAST,
                        driver_id
           ) AS driver_revenue_rank
    FROM fact_rides
    WHERE booking_date BETWEEN :start_date AND :end_date
    GROUP BY driver_id
)
SELECT
    f.ride_id, f.driver_id, f.passenger_id, f.vehicle_id,
    d.driver_name, d.driver_rating AS driver_rating_current,
    dr.driver_total_revenue, dr.driver_revenue_rank,
    p.passenger_name, v.vehicle_model, v.vehicle_color,
    f.ride_status_name, f.payment_method_name,
    COALESCE(f.cancellation_reason_name, 'Not cancelled') AS cancellation_reason_name,
    COALESCE(f.vehicle_type, 'Unknown')      AS vehicle_type,
    COALESCE(f.vehicle_make, 'Unknown')      AS vehicle_make,
    COALESCE(f.pickup_city_name, 'Unknown')  AS pickup_city,
    COALESCE(f.dropoff_city_name, 'Unknown') AS dropoff_city,
    f.booking_date, f.booking_hour,
    dt.day_name, dt.day_of_week, dt.is_weekend, dt.year_month, dt.week_start_date,
    CASE
        WHEN f.booking_hour BETWEEN  5 AND  9 THEN '1 Morning peak (5-9)'
        WHEN f.booking_hour BETWEEN 10 AND 15 THEN '2 Midday (10-15)'
        WHEN f.booking_hour BETWEEN 16 AND 19 THEN '3 Evening peak (16-19)'
        WHEN f.booking_hour BETWEEN 20 AND 23 THEN '4 Night (20-23)'
        ELSE '5 Late night (0-4)'
    END AS time_of_day,
    CASE
        WHEN f.distance_miles <  2 THEN '1 Under 2 mi'
        WHEN f.distance_miles <  5 THEN '2 2-5 mi'
        WHEN f.distance_miles < 10 THEN '3 5-10 mi'
        WHEN f.distance_miles < 20 THEN '4 10-20 mi'
        ELSE '5 20+ mi'
    END AS distance_band,
    CASE
        WHEN f.surge_multiplier <= 1.0 THEN '1 No surge'
        WHEN f.surge_multiplier <= 1.5 THEN '2 Low (1.0-1.5x)'
        WHEN f.surge_multiplier <= 2.0 THEN '3 Medium (1.5-2.0x)'
        ELSE '4 High (2.0x+)'
    END AS surge_band,
    f.distance_miles, f.duration_minutes, f.wait_minutes, f.actual_ride_minutes,
    f.base_fare, f.subtotal, f.tip_amount, f.total_fare, f.fare_per_mile,
    f.surge_multiplier, f.rating,
    f.pickup_latitude, f.pickup_longitude,
    f.is_completed, f.is_cancelled,
    f.tip_amount > 0 AS is_tipped,
    CASE WHEN f.is_cancelled  THEN 1.0 ELSE 0.0 END AS is_cancelled_num,
    CASE WHEN f.tip_amount > 0 THEN 1.0 ELSE 0.0 END AS is_tipped_num,
    CASE WHEN f.tip_amount > 0 THEN 100.0 ELSE 0.0 END AS is_tipped_pct,
    CASE WHEN f.is_completed THEN f.total_fare     END AS completed_revenue,
    CASE WHEN f.is_completed THEN f.tip_amount     END AS completed_tips,
    CASE WHEN f.is_completed THEN f.distance_miles END AS completed_miles
FROM fact_rides AS f
LEFT JOIN driver_current    AS d  USING (driver_id)
LEFT JOIN driver_revenue    AS dr USING (driver_id)
LEFT JOIN passenger_current AS p USING (passenger_id)
LEFT JOIN vehicle_current   AS v USING (vehicle_id)
LEFT JOIN dim_date AS dt ON f.booking_date_key = dt.date_key
WHERE f.booking_date BETWEEN :start_date AND :end_date
;

-- ========================================================================
-- daily_trend  (Daily trend)
-- parameters: start_date, end_date
-- ========================================================================
WITH daily AS (
    SELECT d.calendar_date AS booking_date, d.day_name, d.is_weekend,
           COUNT(f.ride_id) AS rides,
           COUNT_IF(f.is_completed) AS completed_rides,
           COUNT_IF(f.is_cancelled) AS cancelled_rides,
           COALESCE(SUM(CASE WHEN f.is_completed THEN f.total_fare END), 0) AS revenue,
           AVG(CASE WHEN f.is_completed THEN f.total_fare END) AS avg_fare,
           AVG(f.surge_multiplier) AS avg_surge,
           AVG(f.wait_minutes) AS avg_wait_minutes
    FROM dim_date AS d
    LEFT JOIN fact_rides AS f ON f.booking_date_key = d.date_key
    WHERE d.calendar_date BETWEEN :start_date AND :end_date
    GROUP BY d.calendar_date, d.day_name, d.is_weekend
)
SELECT booking_date, day_name, is_weekend, rides, completed_rides, cancelled_rides,
       ROUND(revenue, 2) AS revenue,
       ROUND(avg_fare, 2) AS avg_fare,
       ROUND(avg_surge, 3) AS avg_surge,
       ROUND(avg_wait_minutes, 1) AS avg_wait_minutes,
       ROUND(100.0 * cancelled_rides / NULLIF(rides, 0), 2) AS cancellation_rate_pct,
       ROUND(AVG(revenue) OVER (ORDER BY booking_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7d_avg,
       ROUND(AVG(rides * 1.0) OVER (ORDER BY booking_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS rides_7d_avg
FROM daily
ORDER BY booking_date
;

-- ========================================================================
-- ride_funnel  (Ride funnel)
-- parameters: start_date, end_date
-- ========================================================================
WITH agg AS (
    SELECT COUNT(*) AS booked,
           COUNT_IF(driver_id IS NOT NULL) AS assigned,
           COUNT_IF(pickup_timestamp IS NOT NULL) AS picked_up,
           COUNT_IF(is_completed) AS completed
    FROM fact_rides
    WHERE booking_date BETWEEN :start_date AND :end_date
)
SELECT stage_order, stage, rides,
       ROUND(100.0 * rides / NULLIF(MAX(rides) OVER (), 0), 1) AS pct_of_booked
FROM agg
LATERAL VIEW stack(4,
    1, 'Booked', booked, 2, 'Driver assigned', assigned,
    3, 'Picked up', picked_up, 4, 'Completed', completed
) t AS stage_order, stage, rides
ORDER BY stage_order
;

-- ========================================================================
-- pickup_hotspots  (Pickup hotspots)
-- parameters: start_date, end_date
-- ========================================================================
WITH bounds AS (
    SELECT MIN(pickup_latitude) AS lat_min, MAX(pickup_latitude) AS lat_max,
           MIN(pickup_longitude) AS lon_min, MAX(pickup_longitude) AS lon_max
    FROM fact_rides
    WHERE booking_date BETWEEN :start_date AND :end_date
      AND pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL
),
binned AS (
    SELECT WIDTH_BUCKET(f.pickup_longitude, b.lon_min, b.lon_max, 14) AS lon_bin,
           WIDTH_BUCKET(f.pickup_latitude,  b.lat_min, b.lat_max, 10) AS lat_bin,
           f.total_fare, f.surge_multiplier, f.wait_minutes, f.is_completed
    FROM fact_rides AS f CROSS JOIN bounds AS b
    WHERE f.booking_date BETWEEN :start_date AND :end_date
      AND f.pickup_latitude IS NOT NULL AND f.pickup_longitude IS NOT NULL
)
SELECT lon_bin,
       -lat_bin AS lat_bin,
       COUNT(*) AS rides,
       ROUND(SUM(CASE WHEN is_completed THEN total_fare END), 2) AS revenue,
       ROUND(AVG(surge_multiplier), 2) AS avg_surge,
       ROUND(AVG(wait_minutes), 1) AS avg_wait_minutes
FROM binned
WHERE lon_bin IS NOT NULL AND lat_bin IS NOT NULL
GROUP BY lon_bin, lat_bin
HAVING COUNT(*) >= 5
ORDER BY rides DESC
;
