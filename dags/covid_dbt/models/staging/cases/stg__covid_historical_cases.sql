{{
    config(
        materialized = 'incremental',
        unique_key = 'date'
    )
 }}

SELECT
    date,
    cases
FROM
    {{ source('raw', 'covid_historical') }}

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}