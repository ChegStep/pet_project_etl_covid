{{
    config(
        materialized='incremental',
        unique_key='date',
        strategy='check',
        check_cols=['cases']
    )
}}

WITH source AS (
    SELECT
        date,
        cases,
        cases != LAG(cases) OVER (ORDER BY date) AS did_change
    FROM {{ ref('stg__covid_historical') }}
),

records_with_validity AS (
    SELECT
        date,
        cases,
        date AS valid_from,
        did_change,
        COALESCE(
            LEAD(date) OVER (PARTITION BY CASE WHEN did_change THEN 1 END ORDER BY date),
            '9999-12-31'::date
        ) AS valid_to
    FROM source
)

SELECT
    date,
    cases,
    valid_from,
    valid_to
FROM records_with_validity
WHERE did_change OR valid_to IS NULL