# Module 7 Homework: Streaming with PyFlink & Redpanda

Solutions for the Data Engineering Zoomcamp Module 7 homework.

---

## Setup

```bash
cd ../../../07-streaming/pyflink/
docker-compose up
```

Connect to Postgres:

```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
```

Create landing tables:

```sql
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER
);
```

---

## Question 1. Redpanda version

Check the version of Redpanda inside the container:

```bash
docker exec -it redpanda-1 rpk version
```

**Answer:** <!-- TODO -->

---

## Question 2. Creating a topic

Create a topic named `green-trips` using `rpk`:

```bash
# TODO
```

**Answer (full output):** <!-- TODO -->

---

## Question 3. Connecting to the Kafka server

```python
from kafka import KafkaProducer
import json

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

What is the output of `producer.bootstrap_connected()`?

**Answer:** <!-- TODO -->

---

## Question 4. Sending the trip data

Send the Green 2019-10 dataset to the `green-trips` topic keeping only these columns:
`lpep_pickup_datetime`, `lpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `tip_amount`

**Code used:**

```python
# TODO
```

How long did it take to send and flush?

**Answer:** <!-- TODO -->

---

## Question 5. Sessionization window

Build a session window with a 5-minute gap using `lpep_dropoff_datetime` as the watermark (5 second tolerance). Which pickup and dropoff locations have the longest unbroken streak of taxi trips?

**Answer:** <!-- TODO -->

---

## Submitting

- [Homework submission form](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6)
