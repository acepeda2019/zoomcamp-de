import sys
import pandas as pd
from pathlib import Path

script_dir = Path(__file__).parent
output_dir = script_dir.parent / 'data' / 'output'
output_dir.mkdir(exist_ok=True) # create the directory if it doesn't exist

print("arguments", sys.argv)

day = int(sys.argv[1])
print(f"Running pipeline for day {day}")

df = pd.DataFrame({
    "ID": [1,2,3],
    "name": ["John", "Jane", "Jim"],
    "day": [2,8,2],
    })

df = df.loc[df["day"] == day, :]

if df.empty:
    print(f"No rows with day={day}; exiting.")
    sys.exit(0)

df.to_parquet(output_dir / f'output_day_{day}.parquet')
print(df.head())
print("Pipeline executed successfully")

files = [f.name for f in output_dir.iterdir()]
print(f'Files in {script_dir}: {files}')