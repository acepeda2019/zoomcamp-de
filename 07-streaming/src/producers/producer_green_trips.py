import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import GreenRide, green_ride_from_row


# Download NYC yellow taxi trip data (first 1000 rows)
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'PULocationID',
    'DOLocationID',
    'trip_distance',
    'total_amount',
    'tip_amount',
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'passenger_count',
]
df = pd.read_parquet(url, columns=columns)

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
)
t0 = time.time()

topic_name = 'green-trips'

for _, row in df.iterrows():
    ride = green_ride_from_row(row).serialize()
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')