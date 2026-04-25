with date_spine as (
    -- Date range covers all ClassicModels order dates (2003-01-06 to 2005-05-31).
    -- Extended to full calendar years for clean annual reporting.
    select generate_series::date as date_day
    from generate_series(
        date '2003-01-01',
        date '2006-12-31',
        interval '1 day'
    )
)
select
    date_day                                    as date_id,
    date_day                                    as full_date,
    extract('year'    from date_day)::int       as year,
    extract('quarter' from date_day)::int       as quarter,
    extract('month'   from date_day)::int       as month,
    strftime('%B', date_day)                    as month_name,
    extract('week'    from date_day)::int       as week_of_year,
    extract('day'     from date_day)::int       as day_of_month,
    extract('dow'     from date_day)::int       as day_of_week,
    strftime('%A', date_day)                    as day_name,
    (extract('dow' from date_day) in (0, 6))    as is_weekend
from date_spine
