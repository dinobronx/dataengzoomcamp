{{ config(materialized='view') }}

with tripdata as (select *
                    from {{ source ('staging', 'fhv_tripdata') }}
)
                

select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationid as integer) as pickup_locationid, 
    cast(DOlocationid as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag,
    Affiliated_base_number as affiliated_base_num
from tripdata
{% if var('is_test_run', default=true) %}

limit 100

{% endif %}