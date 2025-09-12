{{
    config(
        materialized = 'table'
    )
}}

SELECT
    cs.date,
    cases,
    deaths,
    recovered
FROM
    {{ ref('stg__covid_historical_cases') }} cs
JOIN {{ ref('stg__covid_historical_deaths') }} dths ON cs.date = dths.date
JOIN {{ ref('stg__covid_historical_recovered') }} rcvd ON cs.date = rcvd.date