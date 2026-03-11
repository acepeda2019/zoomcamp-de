/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

  # Cleaned, deduplicated trip records
columns:
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp (unified across taxi types)"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp (unified across taxi types)"
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "Pickup location ID"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "Dropoff location ID"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Trip fare amount in USD"
    nullable: false
  - name: payment_type
    type: integer
    description: "Raw payment type code from TLC schema"
    nullable: false
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type description"
    nullable: true
  - name: taxi_type
    type: string
    description: "Taxi type (e.g. yellow, green)"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: "UTC timestamp when this record was ingested from the source"
    nullable: false


# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH base AS (
    SELECT
      -- identifiers
      vendor_id
      , tpep_pickup_datetime as pickup_datetime
      , tpep_dropoff_datetime as dropoff_datetime
      , pu_location_id as pickup_location_id
      , do_location_id as dropoff_location_id

      --trip info
      , passenger_count
      , trip_distance
      , ratecode_id
      , store_and_fwd_flag
      , t.payment_type
      , pl.payment_type_name

      --payment info
      , fare_amount
      , extra
      , mta_tax
      , tip_amount
      , tolls_amount
      , improvement_surcharge
      , total_amount
      , congestion_surcharge
      , airport_fee

      -- metadata
      , taxi_type
      , extracted_at

      , ROW_NUMBER() OVER ( PARTITION BY 
        taxi_type
        , vendor_id
        , tpep_pickup_datetime
        , tpep_dropoff_datetime
        , pu_location_id
        , do_location_id
      ORDER BY extracted_at DESC) AS rn

    FROM ingestion.trips t
    LEFT JOIN ingestion.payment_lookup pl
      ON t.payment_type = pl.payment_type_id
    WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
      AND tpep_pickup_datetime < '{{ end_datetime }}'
) 

SELECT *
FROM base
WHERE rn = 1;

bruin run 
--start-date 2026-03-10T00:00:00.000Z 
--end-date 2026-03-10T23:59:59.999999999Z 
--var taxi_types='["blue"]' 
--environment default /Users/acepeda/Documents/GitHub/zoomcamp-de/05-data-platforms/zoomcamp/pipeline/assets/staging/trips.sql