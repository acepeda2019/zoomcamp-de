import dataclasses
import json
from dataclasses import dataclass

import pandas as pd
from typing import Optional


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    pickup_time: int  # epoch milliseconds
    dropoff_time: int  # epoch milliseconds

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def serialize(self) -> bytes:
        return json.dumps(self.to_dict()).encode('utf-8')

@dataclass
class YellowRide(Ride):
    pass

@dataclass
class GreenRide(Ride):
    tip_amount: float
    passenger_count: Optional[int] = None


def yellow_ride_from_row(row: pd.Series) -> YellowRide:
    return YellowRide(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        pickup_time=int(row['tpep_pickup_datetime'].timestamp() * 1000),
        dropoff_time=int(row['tpep_dropoff_datetime'].timestamp() * 1000),
    )

def green_ride_from_row(row: pd.Series) -> GreenRide:
    return GreenRide(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']) if pd.notna(row['passenger_count']) else None,
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
        pickup_time=int(row['lpep_pickup_datetime'].timestamp() * 1000), # epoch milliseconds
        dropoff_time=int(row['lpep_dropoff_datetime'].timestamp() * 1000), # epoch milliseconds
    )

def yellow_ride_deserializer(data: bytes) -> YellowRide:
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return YellowRide(**ride_dict)

def green_ride_deserializer(data: bytes) -> GreenRide:
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return GreenRide(**ride_dict)