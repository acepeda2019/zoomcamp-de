SELECT
    -- identifiers
    dispatching_base_num,
    Affiliated_base_number AS affiliated_base_number,
    
    -- timestamps
    pickup_datetime AS pickup_datetime,
    dropOff_datetime AS dropoff_datetime,

    -- location ids
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag AS sr_flag,

    

FROM {{ source('raw', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL
{{ dev_limit() }}

