SELECT 
    locationid as location_id
    , zone
    , borough
    , service_zone
FROM {{ ref('taxi_zone_lookup') }}