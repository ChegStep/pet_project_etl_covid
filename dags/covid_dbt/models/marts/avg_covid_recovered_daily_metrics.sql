{{
    config(
        materialized = 'table'
    )
 }}

with daily_data as (
    SELECT
        date,
        recovered,
        recovered - LAG(recovered) OVER(ORDER BY date) as new_recovered
    FROM
        {{ ref('stg__covid_historical_recovered') }}
)

SELECT
    date,
    recovered,
    new_recovered,
    round(AVG(new_recovered) OVER(ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) as avg_new_recovered_7d
FROM
    daily_data
ORDER BY date