with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS pickup_month,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.PULocationID as pulocation_id,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.DOLocationID as dolocation_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.SR_Flag AS sr_flag,
    fhv_tripdata.Affiliated_base_number as affiliated_base_num 
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dolocationid = dropoff_zone.locationid