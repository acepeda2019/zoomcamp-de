with source as (
    select *
    from {{ source('raw','yellow_tripdata') }}
    where vendorid is not null 
),

renamed as (
    select
        -- identifiers
        cast(vendorid as integer) as vendor_id,
        cast(ratecodeid as integer) as ratecode_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        
        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        
        -- trip info
        store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as {{ numeric_type() }}) as trip_distance,
        cast(1 as integer) as trip_type,  -- Yellow only does street-hail
        
        -- payment info
        cast(fare_amount as {{ numeric_type() }}) as fare_amount,
        cast(extra as {{ numeric_type() }}) as extra,
        cast(mta_tax as {{ numeric_type() }}) as mta_tax,
        cast(tip_amount as {{ numeric_type() }}) as tip_amount,
        cast(tolls_amount as {{ numeric_type() }}) as tolls_amount,
        cast(0 as {{ numeric_type() }}) as ehail_fee,  -- Yellow doesn't have ehail
        cast(improvement_surcharge as {{ numeric_type() }}) as improvement_surcharge,
        cast(total_amount as {{ numeric_type() }}) as total_amount,
        cast(payment_type as integer) as payment_type,
        cast(congestion_surcharge as {{ numeric_type() }})   as congestion_surcharge
    from source
)

select * from renamed
{{ dev_limit() }}