{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'  )
}}

WITH 
    unioned_data AS (
        select 'Green' as service_type, * from {{ ref('stg_green_tripdata') }}
        union all
        select 'Yellow' as service_type, * from {{ ref('stg_yellow_tripdata') }}
    ),
    
    payment_type AS (
        SELECT payment_type, description 
        FROM {{ ref('payment_type_lookup') }}
    ),

    cleaned_and_enriched AS (
        SELECT 
            -- UUID
            {{ dbt_utils.generate_surrogate_key([
                'vendor_id', 
                'pickup_datetime', 
                'pickup_location_id', 
                'service_type'
            ]) }} as trip_id,
            
            -- identifiers
            u.vendor_id,
            u.service_type,
            u.rate_code_id,

            -- location id's
            u.pickup_location_id,
            u.dropoff_location_id,

            -- timestamps
            u.pickup_datetime,
            u.dropoff_datetime,

            --trip details
            u.store_and_fwd_flag,
            u.passenger_count,
            u.trip_distance,
            u.trip_type,

            -- payment details
            u.fare_amount,
            u.extra,
            u.ehail_fee,
            u.mta_tax,
            u.tip_amount,
            u.tolls_amount,
            u.improvement_surcharge,
            u.total_amount,

            -- enriched payment types
            COALESCE(u.payment_type, 0) as payment_type,
            COALESCE(pt.description, 'UNKNOWN') as payment_type_description

        FROM unioned_data u
        LEFT JOIN payment_type pt
            ON u.payment_type = pt.payment_type
    )

SELECT * FROM cleaned_and_enriched

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY vendor_id, pickup_datetime, pickup_location_id, service_type 
    ORDER BY pickup_datetime DESC) = 1

{{ dev_limit() }}
