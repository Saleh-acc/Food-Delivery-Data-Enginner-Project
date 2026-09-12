-- type of table 
{{ config(
    materialized = 'table'
) }}

WITH time_spine as (
    SELECT explode(sequence(
        to_timestamp('2000-01-01 00:00:00'),
        to_timestamp('2000-01-01 23:59:00'),
        interval 1 minute
    )) as ts
)

SELECT 
    CAST(date_format(ts, 'HHmm') as int) as time_key,
    hour(ts) as hour,
    minute(ts) as minute,
    date_format(ts, 'a') as am_pm,
    date_format(ts, 'hh')as hour_12,
    CASE
    WHEN hour(ts) BETWEEN 11 AND 13   -- lunch rush
      OR hour(ts) BETWEEN 18 AND 21   -- dinner rush
    THEN true
    ELSE false
    END AS is_peak_hour,
     CASE
        WHEN hour(ts) BETWEEN 4  AND 11 THEN 'Morning'
        WHEN hour(ts) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour(ts) BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night' -- 22,23,0–3
    END AS time_of_day

FROM time_spine