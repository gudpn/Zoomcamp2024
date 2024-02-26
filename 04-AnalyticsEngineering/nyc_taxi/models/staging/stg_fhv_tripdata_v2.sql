with 

source as (

    select * from {{ source('staging', 'fhv_tripdata_v2') }}

),

renamed as (

    select
        dispatching_base_num,
        TIMESTAMP_MICROS(CAST(pickup_datetime /1000 as INT64)) as pickup_datetime ,
        TIMESTAMP_MICROS(CAST(dropoff_datetime /1000 as INT64)) as dropoff_datetime ,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
        {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,

        affiliated_base_number

    from source

)



select * from renamed
-- dbt build --select <model.sql> --vars '{is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}