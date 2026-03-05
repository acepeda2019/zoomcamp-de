# Module 1 Homework: Docker & SQL

Solutions for the Data Engineering Zoomcamp Module 1 homework.

---

## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.  
**What's the version of `pip` in the image?**

**Command used:**

```bash
docker run -it --entrypoint /bin/bash zc-docker:v001
pip --version
```

**Answer:** pip 25.3

---

## Question 2. Understanding Docker networking and docker-compose

Given the docker-compose with `db` and `pgadmin` services: **What is the hostname and port that pgadmin should use to connect to the postgres database?**

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

**Answer:** 

- postgres:5432
- db:5432

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
SELECT COUNT(*) as ride_count
FROM green_taxi_data
WHERE 1=1
	AND lpep_pickup_datetime >= DATE('2025-11-01')
	AND lpep_pickup_datetime <  DATE ('2025-12-01')
	AND trip_distance <= 1
```

**Answer:** 8,007

---

## Question 4. Longest trip for each day

**Which pick up day had the longest trip distance?** (Only trips with `trip_distance` < 100 miles; use pick up time.)

**SQL used:**

```sql
SELECT 
	DATE(lpep_pickup_datetime) as pickup_date,
	MAX(trip_distance) as longest_trip,
	SUM(trip_distance) as total_mileage
FROM green_taxi_data
WHERE 1=1
	AND trip_distance < 100
GROUP BY pickup_date
ORDER BY longest_trip DESC
LIMIT 10
```

**Answer:** 2025-11-14

---

## Question 5. Biggest pickup zone

**Which pickup zone had the largest `total_amount` (sum of all trips) on November 18th, 2025?**

**SQL used:**

```sql
SELECT 
	z."LocationID",
	z."Zone",
	COUNT(*) as pickup_count
FROM green_taxi_data d
	RIGHT JOIN taxi_zones z ON d."PULocationID" = z."LocationID"
WHERE 1=1
	AND d.trip_distance < 100
GROUP BY 1,2
ORDER BY pickup_count DESC
LIMIT 10
```

**Answer:** East Harlem North

---

## Question 6. Largest tip

For passengers picked up in **"East Harlem North"** in November 2025, **which drop off zone had the largest tip?** (Name of zone, not ID.)

**SQL used:**

```sql
SELECT 
	d."PULocationID",
	puz."Zone" as PUZone,
	d."DOLocationID",
	doz."Zone" as DOZone,
	MAX(tip_amount) as largest_tip,
	SUM(tip_amount) as total_tips
FROM green_taxi_data d
	LEFT JOIN taxi_zones puz ON d."PULocationID" = puz."LocationID"
	LEFT JOIN taxi_zones doz ON d."DOLocationID" = doz."LocationID"
WHERE 1=1
	AND DATE_TRUNC('month', lpep_pickup_datetime) = '2025-11-01'
	AND trip_distance < 100
	AND "PULocationID" = 74 -- East Harlem North
GROUP BY 1,2,3,4
ORDER BY largest_tip DESC
LIMIT 10
```

**Answer:** Yorkville West

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

**Answer:** 

```
terraform init
terraform apply -auto-approve
terraform destroy
```

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw1)

