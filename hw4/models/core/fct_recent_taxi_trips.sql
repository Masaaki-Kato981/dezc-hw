select *
from {{ ref('fact_trips') }}
where DATETIME(pickup_datetime) >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DBT_DAYS_BACK", "30")) }}' DAY