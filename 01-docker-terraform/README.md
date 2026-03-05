# Module 1 Homework: Docker & SQL

Solutions for the Data Engineering Zoomcamp Module 1 homework.

---

## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.  
**What's the version of `pip` in the image?**

**Command used:**
```bash
# TODO: add your docker run command
```

**Answer:** <!-- e.g. 25.3 -->

---

## Question 2. Understanding Docker networking and docker-compose

Given the docker-compose with `db` and `pgadmin` services: **What is the hostname and port that pgadmin should use to connect to the postgres database?**

**Answer:** <!-- e.g. db:5432 -->

---

## Prepare the Data

**Commands used:**
```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

---

## Question 3. Counting short trips

For trips in November 2025 (`lpep_pickup_datetime` between '2025-11-01' and '2025-12-01', exclusive of upper bound), **how many trips had `trip_distance` ≤ 1 mile?**

**SQL used:**
```sql
-- TODO: your query
```

**Answer:** <!-- e.g. 8,254 -->

---

## Question 4. Longest trip for each day

**Which pick up day had the longest trip distance?** (Only trips with `trip_distance` < 100 miles; use pick up time.)

**SQL used:**
```sql
-- TODO: your query
```

**Answer:** <!-- e.g. 2025-11-23 -->

---

## Question 5. Biggest pickup zone

**Which pickup zone had the largest `total_amount` (sum of all trips) on November 18th, 2025?**

**SQL used:**
```sql
-- TODO: your query
```

**Answer:** <!-- e.g. East Harlem North -->

---

## Question 6. Largest tip

For passengers picked up in **"East Harlem North"** in November 2025, **which drop off zone had the largest tip?** (Name of zone, not ID.)

**SQL used:**
```sql
-- TODO: your query
```

**Answer:** <!-- e.g. LaGuardia Airport -->

---

## Terraform

- Install Terraform on VM/Laptop/Codespace.
- Copy/modify Terraform files to create a GCP Bucket and BigQuery Dataset.

---

## Question 7. Terraform Workflow

Which sequence describes the workflow for:
1. Downloading provider plugins and setting up backend  
2. Generating proposed changes and auto-executing the plan  
3. Removing all resources managed by Terraform  

**Answer:** <!-- e.g. terraform init, terraform apply -auto-approve, terraform destroy -->

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw1)
