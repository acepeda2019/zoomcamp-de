# Commands Reference

---

## Redpanda

### Reset topic (delete + recreate)
```bash
docker exec -it 07-streaming-redpanda-1 rpk topic delete green-trips
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips -p 1 -r 1
```

---

## Flink Jobs

### Submit jobs (run one at a time)
```bash
# Q4 - 5-minute tumbling window
docker exec 07-streaming-jobmanager-1 flink run -py /opt/src/job/homework_5min_tumble_job.py

# Q5 - session window
docker exec 07-streaming-jobmanager-1 flink run -py /opt/src/job/homework_session_job.py

# Q6 - 1-hour tumbling window (tip totals)
docker exec 07-streaming-jobmanager-1 flink run -py /opt/src/job/homework_job.py
```

---

## PostgreSQL

### Create tables
```sql
CREATE TABLE IF NOT EXISTS processed_events_green_flink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  num_trips BIGINT,
  tip_amount DOUBLE PRECISION,
  total_revenue DOUBLE PRECISION,
  PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS processed_events_green_flink_5min (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  PULocationID INT,
  num_trips BIGINT,
  total_revenue DOUBLE PRECISION,
  PRIMARY KEY (window_start, PULocationID)
);

CREATE TABLE IF NOT EXISTS processed_events_green_flink_session (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  PULocationID INT,
  num_trips BIGINT,
  total_tips DOUBLE PRECISION,
  total_revenue DOUBLE PRECISION,
  PRIMARY KEY (window_start, PULocationID)
);
```

### Row counts (all tables)
```sql
SELECT 'processed_events_green' as table_name, count(*) as row_ct FROM processed_events_green
UNION ALL
SELECT 'processed_events_green_flink' as table_name, count(*) as row_ct FROM processed_events_green_flink
UNION ALL
SELECT 'processed_events_green_flink_5min' as table_name, count(*) as row_ct FROM processed_events_green_flink_5min
UNION ALL
SELECT 'processed_events_green_flink_session' as table_name, count(*) as row_ct FROM processed_events_green_flink_session;
```

### Truncate all tables
```sql
TRUNCATE processed_events_green;
TRUNCATE processed_events_green_flink;
TRUNCATE processed_events_green_flink_5min;
TRUNCATE processed_events_green_flink_session;
```

---

## Homework Queries

### Q4 - PULocationID with most trips in a 5-minute window
```sql
SELECT pulocationid, num_trips
FROM processed_events_green_flink_5min
ORDER BY num_trips DESC
LIMIT 5;
```

### Q5 - Longest session (most trips in a single session)
```sql
SELECT pulocationid, num_trips
FROM processed_events_green_flink_session
ORDER BY num_trips DESC
LIMIT 5;
```

#### Session analysis for PULocationID 74
```sql
SELECT pulocationid, window_start, window_end,
  window_end - window_start as session_duration,
  window_start - LAG(window_end, 1) OVER (PARTITION BY pulocationid ORDER BY window_end) as session_gap,
  num_trips
FROM processed_events_green_flink_session
WHERE pulocationid = 74
ORDER BY 1, 2, 3;
```

### Q6 - Hour with highest total tip amount
```sql
SELECT window_start, tip_amount as total_tips
FROM processed_events_green_flink
ORDER BY total_tips DESC
LIMIT 5;
```
