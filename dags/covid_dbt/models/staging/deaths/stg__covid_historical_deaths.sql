{{
    config(
        materialized = 'incremental',
        unique_key = 'date'
    )
 }}

SELECT
    date,
    deaths
FROM
    {{ source('raw', 'covid_historical_deaths') }}

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}