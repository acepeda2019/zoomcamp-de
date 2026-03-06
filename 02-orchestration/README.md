# Module 2 Homework: Workflow Orchestration

Solutions for the Data Engineering Zoomcamp Module 2 homework.

---

## Assignment

Extended existing Kestra flows to include green and yellow taxi data for 2021 using the backfill functionality on the scheduled flow.

---

## Question 1. Uncompressed file size

Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size of the `extract` task output?

This can be viewed in the GCS Bucket

**Answer:** 
- 134.5 MB (GCP) OR 
- 128.3 MiB (Linux)

_MB is base-10 and MiB is base-2 but they resolves to the same number of bytes._


---

## Question 2. Rendered variable value

What is the rendered value of the variable `file` when `taxi=green`, `year=2020`, `month=04`?

![](/02-orchestration/images/kestra-green-apr-2020.png)
**Answer:** green_tripdata_2020-04.csv.

---

## Question 3. Yellow Taxi rows for 2020

How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
```sql
SELECT COUNT(*) as row_ct
FROM `zoomcamp.green_tripdata` 
WHERE filename LIKE '%2020%'
```

**Answer:** 24,648,499

---

## Question 4. Green Taxi rows for 2020

How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?

```sql
SELECT COUNT(*) as row_ct
FROM `zoomcamp.green_tripdata` 
WHERE filename LIKE '%2020%'
```

**Answer:** 1,734,051

---

## Question 5. Yellow Taxi rows for March 2021

How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?

![](/02-orchestration/images/yellow-march-2021-rows.png)

**Answer:** 1,925,152

---

## Question 6. Timezone configuration

How would you configure the timezone to New York in a Schedule trigger?

**Answer:** Add `timezone: America/New_York` to the triggers config.

```yaml
  triggers:
    - id: green_schedule
      type: io.kestra.plugin.core.trigger.Schedule
      cron: "0 9 1 * *"
      timezone: America/New_York
      inputs:
        taxi: green
```

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw2)

