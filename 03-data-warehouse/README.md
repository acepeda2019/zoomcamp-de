# Module 3 Homework: Data Warehousing & BigQuery

Solutions for the Data Engineering Zoomcamp Module 3 homework.

---

## Setup

Load Yellow Taxi Trip Records (January–June 2024) into GCS, then create an external table and a materialized table in BigQuery.

```sql
-- Create external table
-- TODO

-- Create materialized table (no partition/cluster)
-- TODO
```

---

## Question 1. Counting records

What is the count of records for the 2024 Yellow Taxi Data?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both tables. What is the estimated amount of data read for each?

**SQL used:**

```sql
-- External table
-- TODO

-- Materialized table
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 3. Understanding columnar storage

Write a query retrieving only `PULocationID`, then one retrieving both `PULocationID` and `DOLocationID`. Why are the estimated bytes different?

**SQL used:**

```sql
-- Single column
-- TODO

-- Two columns
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 4. Counting zero fare trips

How many records have a `fare_amount` of 0?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 5. Partitioning and clustering

What is the best strategy for a table that always filters on `tpep_dropoff_datetime` and orders by `VendorID`?

**SQL used:**

```sql
-- TODO: Create optimized table
```

**Answer:** <!-- TODO -->

---

## Question 6. Partition benefits

Query distinct VendorIDs between `2024-03-01` and `2024-03-15` on both the materialized table and the partitioned table. What are the estimated bytes for each?

**SQL used:**

```sql
-- Materialized table
-- TODO

-- Partitioned table
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 7. External table storage

Where is the data stored in the External Table?

**Answer:** <!-- TODO -->

---

## Question 8. Clustering best practices

Is it best practice in BigQuery to always cluster your data?

**Answer:** <!-- TODO -->

---

## Question 9. Table scan bytes (no points)

Run `SELECT count(*)` from the materialized table. How many bytes does it estimate? Why?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3)
