import dataclasses
import json
from dataclasses import dataclass

import pandas as pd


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def serialize(self) -> bytes:
        return json.dumps(self.to_dict()).encode('utf-8')


def ride_from_row(row: pd.Series) -> Ride:
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000), # epoch milliseconds
    )


def ride_deserializer(data: bytes) -> Ride:
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)