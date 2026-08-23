WITH per_passenger AS (
    SELECT
        f.passenger_id,
        COUNT(*)                                          AS rides,
        SUM(CASE WHEN f.is_completed THEN f.total_fare END) AS revenue
    FROM uber_project.gold.fact_rides AS f
    WHERE f.booking_date BETWEEN :start_date AND :end_date
    GROUP BY f.passenger_id
)
SELECT
    CASE
        WHEN rides = 1        THEN '1 ride'
        WHEN rides BETWEEN 2 AND 4   THEN '2-4 rides'
        WHEN rides BETWEEN 5 AND 9   THEN '5-9 rides'
        WHEN rides BETWEEN 10 AND 24 THEN '10-24 rides'
        ELSE '25+ rides'
    END                                                   AS ride_band,
    COUNT(*)                                              AS passengers,
    SUM(rides)                                            AS rides,
    ROUND(SUM(revenue), 2)                                AS revenue,
    ROUND(100.0 * SUM(revenue) / NULLIF(SUM(SUM(revenue)) OVER (), 0), 2)
                                                          AS revenue_share_pct,
    ROUND(AVG(revenue), 2)                                AS avg_revenue_per_passenger
FROM per_passenger
GROUP BY 1
ORDER BY MIN(rides);