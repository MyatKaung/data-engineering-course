with green_trips as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow_trips as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        cast(1 as integer) as trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        cast(0 as numeric) as ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),

unioned as (
    select * from green_trips
    union all
    select * from yellow_trips
),

deduped as (
    select distinct on (
        vendor_id,
        pickup_datetime,
        pickup_location_id,
        service_type
    )
        *
    from unioned
    order by
        vendor_id,
        pickup_datetime,
        pickup_location_id,
        service_type,
        dropoff_datetime
),

joined as (
    select
        t.*,
        pz.borough as pickup_borough,
        pz.zone as pickup_zone,
        dz.borough as dropoff_borough,
        dz.zone as dropoff_zone
    from deduped t
    left join {{ ref('dim_zones') }} pz
        on t.pickup_location_id = pz.location_id
    left join {{ ref('dim_zones') }} dz
        on t.dropoff_location_id = dz.location_id
)

select * from joined
