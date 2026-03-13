import os
import requests
from pathlib import Path
from google.cloud import bigquery

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
PROJECT_ID = "dtc-de-course-488600"
DATASET_ID = "ny_taxi_ae"


def get_client():
    # Uses GOOGLE_APPLICATION_CREDENTIALS env var set in ~/.zshrc
    return bigquery.Client(project=PROJECT_ID)


YEARS = {
    "yellow": [2019, 2020],
    "green": [2019, 2020],
    "fhv": [2019],
}


def download_and_convert_files(taxi_type):
    import duckdb

    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in YEARS[taxi_type]:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            print(f"Downloading {csv_gz_filename}...")
            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}', strict_mode=false, ignore_errors=true))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")


def merge_parquet_files(taxi_type):
    import duckdb

    data_dir = Path("data") / taxi_type
    merged_path = data_dir / f"{taxi_type}_tripdata_merged.parquet"

    if merged_path.exists():
        merged_path.unlink()

    print(f"Merging parquet files for {taxi_type}...")
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{data_dir}/*.parquet', union_by_name=true)
        )
        TO '{merged_path}' (FORMAT PARQUET)
    """)
    con.close()
    print(f"Merged into {merged_path}")
    return merged_path


def load_to_bigquery(taxi_type, merged_path):
    client = get_client()

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{taxi_type}_tripdata"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    print(f"Loading {merged_path.name} into {table_id}...")
    with open(merged_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
    print(f"Finished: {table.num_rows:,} rows in {table_id}")


if __name__ == "__main__":
    for taxi_type in [
        # "yellow",
        # "green",
        "fhv",
    ]:
        download_and_convert_files(taxi_type)
        merged_path = merge_parquet_files(taxi_type)
        load_to_bigquery(taxi_type, merged_path)
