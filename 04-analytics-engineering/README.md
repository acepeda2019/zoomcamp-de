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

**Answer:** <!-- TODO -->

---

## Question 2. dbt Tests

A new value `6` appears in source data for a column with `accepted_values: [1, 2, 3, 4, 5]`. What happens when you run `dbt test --select fct_trips`?

**Answer:** <!-- TODO -->

---

## Question 3. Counting records in `fct_monthly_zone_revenue`

What is the count of records in the `fct_monthly_zone_revenue` model?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 4. Best performing zone for Green Taxis (2020)

Which pickup zone had the highest total revenue for Green taxi trips in 2020?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 5. Green Taxi trip counts (October 2019)

What is the total number of trips for Green taxis in October 2019?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Question 6. Staging model for FHV data

Create `stg_fhv_tripdata` filtering out null `dispatching_base_num`. What is the count of records?

**SQL used:**

```sql
-- TODO
```

**Answer:** <!-- TODO -->

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw4)
