-- raw data
with tripdata as (
    select *
    from {{ ref('dim_fhv_trips') }}
),
-- get trip duration
trip_time as (
    select *,
           TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration
    from tripdata
),
-- get trip duration p90
trip_time_perc as (
    select 
        *,
        ROUND(PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY pickup_year, pickup_month, CAST(pulocation_id as STRING), CAST(dolocation_id as STRING)), 2) as trip_dur_p90
    from trip_time
),
-- get distinct observations
trip_time_distinct as (
    select distinct pickup_year,
                    pickup_month,
                    pulocation_id,
                    pickup_zone,
                    dolocation_id,
                    dropoff_zone,
                    trip_dur_p90
    from trip_time_perc
),
-- Trips pickuped from NWK, Soho and Yorkvill East in 11/2019
trip_select AS (
    select pickup_year,
           pickup_month,
           pickup_zone,
           dropoff_zone,
           trip_dur_p90
    from trip_time_distinct
    where pickup_year = 2019 and 
          pickup_month = 11 and
          pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
),
-- Rank observations by pickup zone in descending trip p90 order
trip_ranking as (
    select *,
           rank() over (partition by pickup_zone order by trip_dur_p90 desc) as trip_rank
    from trip_select
)
-- Find 2nd longest trip p90 for each pickup zone
select *
from trip_ranking
where trip_rank = 2

