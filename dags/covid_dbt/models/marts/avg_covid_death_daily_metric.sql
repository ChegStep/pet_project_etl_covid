{{
    config(
        materialized = 'table'
    )
 }}

with daily_data as (
    SELECT
        date,
        deaths,
        deaths - LAG(deaths) OVER(ORDER BY date) as new_deaths
    FROM
        {{ ref('stg__covid_historical_deaths') }}
)

SELECT
    date,
    deaths,
    new_deaths,
    round(AVG(new_deaths) OVER(ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) as avg_new_deaths_7d
FROM
    daily_data
ORDER BY date