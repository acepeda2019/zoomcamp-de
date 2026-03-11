/* @bruin

name: reports.trips_report
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - staging.trips

columns:
  - name: pickup_date
    type: date
    description: Date of trip pickup (derived from pickup_datetime)
    primary_key: true
  - name: taxi_type
    type: string
    description: Taxi type (e.g. yellow, green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Human-readable payment type
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips for the given date, taxi type, and payment type
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Total fare amount for the group


@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

-- TODO: aggregate trips by date, taxi type, and payment type
SELECT 
    pickup_datetime::date as pickup_date,
    taxi_type,
    payment_type_name,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY pickup_date, taxi_type, payment_type_name
