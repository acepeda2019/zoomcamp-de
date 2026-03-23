# Module 6 Homework: Batch Processing with Spark

Solutions for the Data Engineering Zoomcamp Module 6 homework.

---

## Setup

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

---

## Question 1. Spark version

Install Spark, run PySpark, create a local session, and execute `spark.version`. What is the output?

**Code used:**

```python
# Start the local cluster
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh

# Start the SparkSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("homework") \
    .getOrCreate()

# Verify Spark Version
spark.version
```

**Answer:**  4.1.1

---

## Question 2. Yellow November 2025

Read the November 2025 Yellow data into a Spark DataFrame, repartition to 4 partitions, and save to parquet. What is the average file size?

**Code used:**

```python
df_raw.coalesce(4) \
    .write \
    .mode("overwrite") \
    .parquet("../data/homework/parquet/")

df_raw.repartition(4) \
    .write \
    .mode("overwrite") \
    .parquet("../data/homework/parquet/")
```

**Answer:** 

- coalesce(): 21 MB

- repartition(): 24 MB

Repartitioning hashes the whole row and reshuffles to create evenly partitioned files. Coalesce perserves the row order. Parquet compresses better when similar/related rows are adjacent. 

---

## Question 3. Count records

How many taxi trips started on November 15th?

**Code used:**

```python
df\
    .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))\
    .filter(F.col("pickup_date") == '2025-11-15') \
    .groupBy(F.col("pickup_date")) \
    .count()\
    .show(5)
```

**Answer:** 162,604

---

## Question 4. Longest trip

What is the length of the longest trip in the dataset in hours?

**Code used:**

```python
duration_hours = F.round(
    ( F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime") )  / 3600,
    2
)

df \
    .select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance") \
    .withColumn("duration_hours", duration_hours) \
    .orderBy("duration_hours", ascending=False) \
    .show(5)
```

**Answer:** 121.17 Miles @ 90.65 Hrs

---

## Question 5. Spark UI port

Which local port does the Spark UI run on?

**Answer:** 4040

---

## Question 6. Least frequent pickup zone

Using the zone lookup data and Yellow November 2025 data, what is the name of the least frequent pickup location zone?

**Code used:**

```python
df \
    .select("PULocationID") \
    .groupBy(F.col("PULocationID")) \
    .agg(F.count("*").alias("ride_count")) \
    .join(df_zones, on= F.col("PULocationID").cast("int") == F.col("LocationID").cast("int"), how="left") \
    .select("PULocationID", "Zone", "ride_count") \
    .orderBy("ride_count", ascending=True) \
    .show(5)
```

**Answer:** 

```
+------------+---------------------------------------------+----------+
|PULocationID|Zone                                         |ride_count|
+------------+---------------------------------------------+----------+
|105         |Governor's Island/Ellis Island/Liberty Island|1         |
|5           |Arden Heights                                |1         |
|84          |Eltingville/Annadale/Prince's Bay            |1         |
```

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6)

