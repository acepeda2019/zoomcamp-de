# Module 5 Homework: Data Platforms with Bruin

Solutions for the Data Engineering Zoomcamp Module 5 homework.

---

## Setup

```bash
curl -LsSf https://getbruin.com/install/cli | sh
bruin init zoomcamp my-pipeline
```

---

## Question 1. Bruin pipeline structure

What are the required files/directories in a Bruin project?

**Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

---

## Question 2. Materialization strategies

Which incremental strategy is best for processing a specific time interval by deleting and inserting data for that period?

**Answer:** time_interval

---

## Question 3. Pipeline variables

How do you override the `taxi_types` array variable to only process yellow taxis when running the pipeline?

**Command:**

```bash
bruin run --var 'taxi_types=["yellow"]'
```

**Answer:** 

---

## Question 4. Running with dependencies

You modified `ingestion/trips.py` and want to run it plus all downstream assets. Which command should you use?

**Command:**

```bash
bruin run ingestion/trips.py --downstream
```

**Answer:** 

---

## Question 5. Quality checks

Which quality check ensures `pickup_datetime` never has NULL values?

**Answer:** 

```yaml
name: pickup_datetime
type: datetime
checks:
    - name: not_null
```

---

## Question 6. Lineage and dependencies

Which Bruin command visualizes the dependency graph between assets?

**Command:**

```bash
 bruin  lineage
```

**Answer:** 

---

## Question 7. First-time run

What flag ensures tables are created from scratch on a first-time run?

**Command:**

```bash
--full-refresh
```

**Answer:** 

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5)

```

```

