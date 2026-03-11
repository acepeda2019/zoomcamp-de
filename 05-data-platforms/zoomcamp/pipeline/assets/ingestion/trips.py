"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: tpep_pickup_datetime
    type: timestamp
    description: "Vendor pickup timestamp for yellow taxis"
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "Vendor dropoff timestamp for yellow taxis"
  - name: lpep_pickup_datetime
    type: timestamp
    description: "Vendor pickup timestamp for green taxis"
  - name: lpep_dropoff_datetime
    type: timestamp
    description: "Vendor dropoff timestamp for green taxis"
  - name: PULocationID
    type: integer
    description: "Pickup location ID"
  - name: DOLocationID
    type: integer
    description: "Dropoff location ID"
  - name: fare_amount
    type: float
    description: "Base fare in USD"
  - name: payment_type
    type: integer
    description: "Numeric payment type code from TLC schema"
  - name: taxi_type
    type: string
    description: "Taxi type as provided by pipeline variable (e.g. yellow, green)"
  - name: extracted_at
    type: timestamp
    description: "UTC timestamp when this record was ingested"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import os
import json
from datetime import datetime, timedelta

import pandas as pd


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    Implement ingestion of NYC Taxi trip data using Bruin runtime context.

    - Uses BRUIN_START_DATE / BRUIN_END_DATE to determine the run window.
    - Uses the `taxi_types` pipeline variable (from BRUIN_VARS) to decide which taxi types to ingest.
    - Fetches raw parquet files from the TLC public endpoint and concatenates them.
    - Adds an `extracted_at` column for lineage/debugging.
    - Returns a pandas DataFrame that Bruin materializes using the configured strategy.
    """
    # Parse required Bruin environment variables for the current run window.
    # BRUIN_START_DATE / BRUIN_END_DATE are YYYY-MM-DD strings.
    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d").date()
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d").date()

    # Pipeline variables (JSON string). We expect an optional "taxi_types" array.
    bruin_vars_raw = os.environ.get("BRUIN_VARS", "{}")
    bruin_vars = json.loads(bruin_vars_raw)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # Generate a list of month strings (YYYY-MM) between start and end date (inclusive).
    months = []
    current_date = start_date.replace(day=1)
    while current_date <= end_date:
        months.append(current_date.strftime("%Y-%m"))
        current_date = (current_date + timedelta(days=31)).replace(day=1)

    # Fetch raw parquet files from the TLC public endpoint for each taxi_type/month.
    # Keep the data in its rawest format; only add lineage columns.
    dataframes = []
    for month in months:
        for taxi_type in taxi_types:
            url = (
                "https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{taxi_type}_tripdata_{month}.parquet"
            )
            df = pd.read_parquet(url)
            df["taxi_type"] = taxi_type
            df["extracted_at"] = datetime.utcnow()
            dataframes.append(df)

    if not dataframes:
        # No data for the requested window; return an empty DataFrame.
        return pd.DataFrame()

    final_dataframe = pd.concat(dataframes, ignore_index=True)
    return final_dataframe
