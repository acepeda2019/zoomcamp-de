# Module 7 Homework: Streaming with PyFlink & Redpanda

Solutions for the Data Engineering Zoomcamp Module 7 homework.

---

## Setup

```bash
cd 07-streaming/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on `localhost:9092`
- Flink Job Manager at [http://localhost:8081](http://localhost:8081)
- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

Connect to Postgres:

```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
```

---

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it 07-streaming-redpanda-1 rpk version
```

**Answer:**

```bash
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 47 54 Thu
OS/Arch:     linux/arm64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

---

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```bash
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
```

Write a producer to send the green taxi data to this topic. Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic. Convert datetime columns to strings before serializing to JSON.

How long did it take to send the data?

- 10 seconds
- 60 seconds
- **120 seconds**
- 300 seconds

**Answer:**

```
Removed time.sleep(0.01): 3.33 seconds
With time.sleep(0.01): 621.86 seconds
```

---

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic (set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- 8506
- 9506

**Answer:**

```sql
SELECT 
    CASE WHEN trip_distance > 5 THEN'> 5' ELSE '<= 5' END as trip_distance_group, 
    count(*) as trip_count
FROM processed_events_green
GROUP BY trip_distance_group;
```

```bash
+---------------------+------------+
| trip_distance_group | trip_count |
|---------------------+------------|
| > 5                 | 8506       |
| <= 5                | 40910      |
+---------------------+------------+
```

---

## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, adapt the workshop code to work with the green taxi data:

- Topic name: `green-trips`
- Datetime columns use `lpep_` prefix
- Handle timestamps as strings (not epoch milliseconds)

Convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Important notes:

- Place job files in `07-streaming/src/job/` — mounted into Flink containers at `/opt/src/job/`
- Submit jobs with: `docker exec -it 07-streaming-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- Set parallelism to 1 (`env.set_parallelism(1)`) — the topic has 1 partition
- Let jobs run for a minute or two until results appear in PostgreSQL, then query

---

## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute tumbling window to count trips per `PULocationID`.

Write results to a PostgreSQL table with columns: `window_start`, `PULocationID`, `num_trips`.

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- 74
- 75
- 166

**Answer:**

---

## Question 5. Session window - longest streak

Create a Flink job using a session window with a 5-minute gap on `PULocationID`, using `lpep_pickup_datetime` as the event time with a 5-second watermark tolerance.

Write results to a PostgreSQL table and find the `PULocationID` with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- 81

**Answer:**

---

## Question 6. Tumbling window - largest tip

Create a Flink job using a 1-hour tumbling window to compute the total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

**Answer:**

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7)

