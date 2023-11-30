SELECT
    day_date,
    (uniqExactIf(user_id, day_date = day_date) / 
     uniqExactIf(user_id, day_date >= day_date - interval 6 day AND day_date <= day_date)) * 100 AS Sticky_Factor
FROM mixed_events
GROUP BY day_date
ORDER BY day_date
