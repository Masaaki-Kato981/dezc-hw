## Homework 4
Code using this directory (hw4)

### Questions:
1. Understanding dbt model resolution
    - `select * from myproject.my_nyc_tripdata.ext_green_taxi`
2. dbt Variables & Dynamic Models
    - `Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`
    - double checked using `models/core/fct_recent_taxi_trips.sql`
3. dbt Data Lineage and Execution
    - `dbt run --select +models/core/`
4. dbt Macros and Jinja
    - `Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile`
    - `When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET`
    - `When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET`
    - `When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET`
5. Taxi Quarterly Revenue Growth
    - build model in `models/core/fct_taxi_trips_quarterly_revenue.sql`
    - `green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}`
6. P97/P95/P90 Taxi Monthly Fare
    - build model in `models/core/fct_taxi_trips_monthly_fare_p95.sql`
    - `green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}`
7. Top #Nth longest P90 travel time Location for FHV
    - build models `models/stagning/stg_fhv_tripdata.sql`, `models/core/dim_fhv_trips.sql`, `models/core/fct_fhv_monthly_zone_traveltime_p90.sql`
    - `LaGuardia Airport, Chinatown, Garment District`
