{{
    config(
        materialized = 'table'
    )
 }}

with daily_data as (
    SELECT
        date,
        cases,
        cases - LAG(cases) OVER(ORDER BY date) as new_cases
    FROM
        {{ ref('stg__covid_historical_cases') }}
)

SELECT
    date,
    cases,
    new_cases,
    round(AVG(new_cases) OVER(ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) as avg_new_cases_7d
FROM
    daily_data
ORDER BY date