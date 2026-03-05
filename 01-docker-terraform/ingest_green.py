import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

df = pd.read_parquet('green_tripdata_2025-11.parquet')
df.to_sql('green_taxi_data', engine, if_exists='replace')
df.info()

df_zones = pd.read_csv('taxi_zone_lookup.csv')
df_zones.to_sql('taxi_zones', engine, if_exists='replace')
df_zones.info()

