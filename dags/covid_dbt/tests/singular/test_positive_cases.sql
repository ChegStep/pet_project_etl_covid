with daily_changes as (
    select
        date,
        cases,
        cases - lag(cases) over (order by date) as daily_changes
    from {{ ref('stg__covid_historical_cases') }}
    where date >= '2020-01-01'
),
negative_changes as (
        select
            date,
            cases,
            daily_changes
        from daily_changes
        where daily_changes < 0
        and date > '2020-01-01'
    )

select
    date,
    cases,
    daily_changes
from negative_changes
where abs(daily_changes) > 100
