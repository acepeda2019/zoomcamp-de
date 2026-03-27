import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import GreenRide, green_ride_deserializer
import psycopg2
from datetime import datetime

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-to-postgres',
    value_deserializer=green_ride_deserializer
)

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()


cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_events_green (
        id SERIAL PRIMARY KEY,
        PULocationID INT,
        DOLocationID INT,
        trip_distance FLOAT,
        total_amount FLOAT,
        tip_amount FLOAT,
        passenger_count INT,
        pickup_dt TIMESTAMP,
        dropoff_dt TIMESTAMP
        )
""")

print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
try:
    for message in consumer:
        ride = message.value
        pickup_dt = datetime.fromtimestamp(ride.pickup_time / 1000)
        dropoff_dt = datetime.fromtimestamp(ride.dropoff_time / 1000)
        cur.execute("""
            INSERT INTO processed_events_green(
                PULocationID,
                DOLocationID,
                trip_distance,
                total_amount,
                tip_amount,
                pickup_dt,
                dropoff_dt,
                passenger_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ride.PULocationID,
                ride.DOLocationID,
                ride.trip_distance,
                ride.total_amount,
                ride.tip_amount,
                pickup_dt,
                dropoff_dt,
                ride.passenger_count
            )
        )
        count += 1
        if count % 100 == 0:
            print(f"Inserted {count} rows...")

except KeyboardInterrupt:
    print("Shutting down ...")
finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer closed, cursor closed, connection closed")