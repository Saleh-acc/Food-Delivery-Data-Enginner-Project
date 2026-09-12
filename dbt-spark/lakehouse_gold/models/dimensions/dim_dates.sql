-- type of table 
{{ config(
    materialized = 'table'
) }}



WITH date_spine AS (
    SELECT explode(sequence(
        to_date('2023-01-1'),
        to_date('2030-12-31'),
        interval 1 day
    )) as full_date
)


SELECT 
    CAST(date_format(full_date, 'yyyyMMdd') as int) as date_key,
    full_date,
    year(full_date) as year,
    month(full_date) as month,
    day(full_date) AS day,
    quarter(full_date) as quarter,
    date_format(full_date, 'MMMM') as month_name,
    date_format(full_date, 'EEEE') AS day_of_week,
    weekofyear(full_date) AS week_of_year,
    CASE
        WHEN date_format(full_date, 'E') IN ('Sat', 'Fri')
        THEN true
        ELSE false
    END AS is_weekend,
    CASE
        WHEN date_format(full_date, 'E') IN ('Sat', 'Fri')
        THEN false
        ELSE true
    END AS is_workday,
    false AS is_holiday

FROM date_spine

