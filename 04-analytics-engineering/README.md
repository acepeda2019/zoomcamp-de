# Module 4 Homework: Analytics Engineering with dbt

Solutions for the Data Engineering Zoomcamp Module 4 homework.

---

## Setup

```bash
dbt build --target prod
```

---

## Question 1. dbt Lineage and Execution

If you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer:** Only int_trips_unioned

---

## Question 2. dbt Tests

A new value `6` appears in source data for a column with `accepted_values: [1, 2, 3, 4, 5]`. What happens when you run `dbt test --select fct_trips`?

**Answer:** dbt will fail the test, returning a non-zero exit code

---

## Question 3. Counting records in `fct_monthly_zone_revenue`

What is the count of records in the `fct_monthly_zone_revenue` model?

**SQL used:**

```sql
BQ SCHEMA DETAILS
```

**Answer:** 12,184

---

## Question 4. Best performing zone for Green Taxis (2020)

Which pickup zone had the highest total revenue for Green taxi trips in 2020?

**SQL used:**

```sql
SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
FROM `dtc-de-course-488600.ny_taxi_ae.fct_monthly_zone_revenue` 
WHERE 1=1
  AND service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 10
```

**Answer:** East Harlem North | $1,817,163.89

---

## Question 5. Green Taxi trip counts (October 2019)

What is the total number of trips for Green taxis in October 2019?

**SQL used:**

```sql
SELECT SUM(total_monthly_trips) as total_trips
FROM `dtc-de-course-488600.ny_taxi_ae.fct_monthly_zone_revenue` 
WHERE 1=1
  AND service_type = 'Green'
  AND revenue_month = '2019-10-01'
```

**Answer:** 384,624

---

## Question 6. Staging model for FHV data

Create `stg_fhv_tripdata` filtering out null `dispatching_base_num`. What is the count of records?

**SQL used:**

```sql
SELECT EXTRACT(YEAR FROM pickup_datetime) as year, COUNT(1) as ct
FROM `dtc-de-course-488600.ny_taxi_ae.stg_fhv_tripdata` 
GROUP BY 1
```

**Answer:** 43,244,693

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw4)

