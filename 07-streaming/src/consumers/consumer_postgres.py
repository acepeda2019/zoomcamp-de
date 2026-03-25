import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import Ride, ride_deserializer
import psycopg2
from datetime import datetime


server = 'localhost:9092'
topic_name = 'rides'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-to-postgres',
    value_deserializer=ride_deserializer
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


print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
try:
    for message in consumer:
        ride = message.value
        pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
        cur.execute("""
            INSERT INTO processed_events(
                PULocationID,
                DOLocationID,
                trip_distance,
                total_amount,
                pickup_datetime,
                topic,
                partition,
                offset_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (topic, partition, offset_id) DO NOTHING
            """,
            (ride.PULocationID, ride.DOLocationID,
            ride.trip_distance, ride.total_amount, pickup_dt,
            message.topic, message.partition, message.offset)
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