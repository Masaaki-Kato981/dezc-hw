with trips_data as (
    select * from {{ ref('fact_trips') }}
    -- where pickup_year = 2019 OR pickup_year = 2020
),
quarterly_revenue as (

    select
        service_type,
        pickup_year_quarter,
        sum(total_amount) as total_revenue
    from trips_data
    group by service_type, pickup_year_quarter

),
yoy_revenue AS (
    select 
        service_type,
        pickup_year_quarter,
        total_revenue,
        lag(total_revenue, 4) over (partition by service_type order by pickup_year_quarter) AS prev_year_revenue,
        total_revenue - lag(total_revenue, 4) over (partition by service_type order by pickup_year_quarter) as yoy_revenue_change
    from quarterly_revenue
    where pickup_year_quarter LIKE '%2019%' OR pickup_year_quarter LIKE '%2020%'
)
select *,
       round(yoy_revenue_change*100/prev_year_revenue,2) AS yoy_growth
from yoy_revenue
order by service_type, pickup_year_quarter
