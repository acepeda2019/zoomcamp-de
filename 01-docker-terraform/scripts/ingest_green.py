import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

data_dir = Path(__file__).parent.parent / 'data'

engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

df = pd.read_parquet(data_dir / 'green_tripdata_2025-11.parquet')
df.to_sql('green_taxi_data', engine, if_exists='replace')
df.info()

df_zones = pd.read_csv(data_dir / 'taxi_zone_lookup.csv')
df_zones.to_sql('taxi_zones', engine, if_exists='replace')
df_zones.info()

