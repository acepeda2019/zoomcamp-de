# Module 3 Homework: Data Warehousing & BigQuery

Solutions for the Data Engineering Zoomcamp Module 3 homework.

---

## Setup

Load Yellow Taxi Trip Records (January–June 2024) into GCS, then create an external table and a materialized table in BigQuery.

```sql
-- CREATE EXTERNAL TABLE
CREATE SCHEMA IF NOT EXISTS `dtc-de-course-488600.ny_taxi`;

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-488600.ny_taxi.yellow_tripdata_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-course-488600-dezoomcamp-hw3-2025/yellow_tripdata_2024-*.parquet']
);

-- CREATE BQ NATIVE TABLE
CREATE OR REPLACE TABLE `dtc-de-course-488600.ny_taxi.yellow_tripdata` AS
SELECT * FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata_ext`;
```

---

## Question 1. Counting records

What is the count of records for the 2024 Yellow Taxi Data?

**SQL used:**

```sql
SELECT COUNT(*) as record_ct
FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata` 
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024
```

**Answer:** 20,332,057

---

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both tables. What is the estimated amount of data read for each?

**SQL used:**

```sql
-- External table
SELECT COUNT(DISTINCT(PULocationID)) as pluids
FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata_ext` ;

-- Materialized table
SELECT COUNT(DISTINCT(PULocationID)) as pluids
FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata` ;
```

**Answer:**

- External: 0MB
- Native: 155.12MB

---

## Question 3. Understanding columnar storage

Write a query retrieving only `PULocationID`, then one retrieving both `PULocationID` and `DOLocationID`. Why are the estimated bytes different?

**SQL used:**

```sql
SELECT 
  PULocationID
  -- , DOLocationID
FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata` ;
```

**Answer:** 
Estimate | Actual Processed

- Single: 465.46MB | 155.12MB
- Double: 620.49MB | 310.24MB

BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

---

## Question 4. Counting zero fare trips

How many records have a `fare_amount` of 0?

**SQL used:**

```sql
SELECT COUNT(*) as ct
FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata` 
WHERE fare_amount = 0;
```

**Answer:** 8,333

---

## Question 5. Partitioning and clustering

What is the best strategy for a table that always filters on `tpep_dropoff_datetime` and orders by `VendorID`?

**SQL used:**

```sql
CREATE OR REPLACE TABLE `dtc-de-course-488600.ny_taxi.yellow_tripdata_opt` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata`
```

**Answer:** 

- Partition by tpep_dropoff_datetime and Cluster on VendorID

---

## Question 6. Partition benefits

Query distinct VendorIDs between `2024-03-01` and `2024-03-15` on both the materialized table and the partitioned table. What are the estimated bytes for each?

**SQL used:**

```sql
SELECT DISTINCT VendorID
-- FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata` -- Native
FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata_opt` -- Optimized
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
```

**Answer:** 

- Native: 310.24MB
- Optimized: 26.84MB

---

## Question 7. External table storage

Where is the data stored in the External Table?

**Answer:** GCP Bucket

---

## Question 8. Clustering best practices

Is it best practice in BigQuery to always cluster your data?

**Answer:** False

---

## Question 9. Table scan bytes (no points)

Run `SELECT count(*)` from the materialized table. How many bytes does it estimate? Why?

**SQL used:**

```sql
SELECT COUNT(*) FROM `dtc-de-course-488600.ny_taxi.yellow_tripdata`
```

**Answer:** No columns are selected, but also it's optimized for big query so it already knows the row count on loading as metadata.

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3)

