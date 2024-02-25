{{ config(materialized="table") }}

with
  
    fhv_tripdata_v2 as (
        select *, 'fhv' as service_type from {{ ref("stg_fhv_tripdata_v2") }}
    ),
    trips_unioned as (

        select * from fhv_tripdata_v2
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select


    trips_unioned.service_type,
    trips_unioned.dispatching_base_num,
    trips_unioned.pickup_datetime as pickup_datetime,
    trips_unioned.dropoff_datetime as dropoff_datetime,
    trips_unioned.pickup_locationid as pickup_locationid,
    trips_unioned.dropoff_locationid as dropoff_locationid,
    trips_unioned.sr_flag as sr_flag,
    trips_unioned.affiliated_base_number,

    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,

    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone

from trips_unioned

inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
where pickup_datetime between '2019-01-01' and '2019-12-31' 