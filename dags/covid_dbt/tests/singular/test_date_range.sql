with date_gaps as (
    select
        date,
        lag(date) over(order by date) as previous_date,
        date - lag(date) over(order by date) as day_gap
    from {{ ref('stg__covid_historical_cases') }}
    where date >= '2020-01-01'
)

select
    date,
    previous_date,
    day_gap
from date_gaps
where day_gap > 1