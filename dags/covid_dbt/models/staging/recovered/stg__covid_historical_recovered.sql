{{
    config(
        materialized = 'incremental',
        unique_key = 'recovered'
    )
 }}

SELECT
    date,
    recovered
FROM
    {{ source('raw', 'covid_historical_recovered') }}

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}