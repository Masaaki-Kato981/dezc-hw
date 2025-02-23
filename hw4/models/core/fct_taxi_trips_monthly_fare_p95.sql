with trips_data as (
    select * 
    from {{ ref('fact_trips') }}
    where fare_amount > 0 and 
          trip_distance > 0 and 
          payment_type_description in ('Cash', 'Credit card')
),
perc_fare as (
    select 
    distinct service_type,
    pickup_year,
    pickup_month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS fare_p97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS fare_p95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS fare_p90
    from trips_data

)
select *
from perc_fare
where pickup_year = 2020 and pickup_month = 4