#!/usr/bin/env python
# coding: utf-8

import argparse
import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampNTZType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = "dataproc-temp-us-central1-336855667876-wv5ilslp"


GREEN_SCHEMA = StructType([
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampNTZType(), True),
    StructField("lpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", DoubleType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
])


def process_green_data(spark: SparkSession, input_green: str) -> DataFrame:
    logger.info(f"Processing green data from {input_green}")
    return spark.read.schema(GREEN_SCHEMA).parquet(input_green) \
        .withColumnsRenamed({
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
        }) \
        .withColumn("service_type", F.lit("green"))


YELLOW_SCHEMA = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
])


def process_yellow_data(spark: SparkSession, input_yellow: str) -> DataFrame:
    logger.info(f"Processing yellow data from {input_yellow}")
    return spark.read.schema(YELLOW_SCHEMA).parquet(input_yellow) \
        .withColumnsRenamed({
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
        }) \
        .withColumn("service_type", F.lit("yellow"))


def union_dataframes(green_df: DataFrame, yellow_df: DataFrame) -> DataFrame:
    common_columns = [column for column in green_df.columns if column in yellow_df.columns]
    return green_df.select(common_columns).union(yellow_df.select(common_columns))


def create_revenue_report(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("taxi_data")
    return spark.sql("""
        SELECT
            -- Revenue grouping
            PULocationID AS revenue_zone,
            date_trunc('month', pickup_datetime) AS revenue_month,
            service_type,

            -- Revenue calculation
            SUM(fare_amount) AS revenue_monthly_fare,
            SUM(extra) AS revenue_monthly_extra,
            SUM(mta_tax) AS revenue_monthly_mta_tax,
            SUM(tip_amount) AS revenue_monthly_tip_amount,
            SUM(tolls_amount) AS revenue_monthly_tolls_amount,
            SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
            SUM(total_amount) AS revenue_monthly_total_amount,
            SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

            -- Additional calculations
            AVG(passenger_count) AS avg_monthly_passenger_count,
            AVG(trip_distance) AS avg_monthly_trip_distance
        FROM taxi_data
        GROUP BY 1, 2, 3
    """)


def load_to_bigquery(df: DataFrame, output: str) -> None:
    logger.info(f"Loading results to {output}")
    df.write\
        .format('bigquery') \
        .option('table', output) \
        .save()


def main(input_green: str, input_yellow: str, output: str) -> None:
    spark = SparkSession.builder \
        .appName("taxi-data") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()
    spark.conf.set('temporaryGcsBucket', BUCKET)

    green_df = process_green_data(spark, input_green)
    yellow_df = process_yellow_data(spark, input_yellow)
    df_taxi = union_dataframes(green_df, yellow_df)
    df_result = create_revenue_report(spark, df_taxi)
    load_to_bigquery(df_result, output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-green', type=str, required=True)
    parser.add_argument('--input-yellow', type=str, required=True)
    parser.add_argument('--output', type=str, required=True)
    args = parser.parse_args()

    main(args.input_green, args.input_yellow, args.output)

