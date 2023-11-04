from pathlib import Path
import os
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket




@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """"Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['lpep_pickup_datetime']  = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime']  = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as Parquet file"""
    path = Path(f"../../data/{color}")
    if not path.exists():
        os.mkdir(path)

    path = path / f"{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")
    return path
@task(log_prints=True)
def write_gcs(path: Path, color: str, file_name: str):
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=f"data/{color}/{file_name}.parquet"
    )

@flow()
def etl_web_to_gcs() -> None:
    """The Main ETL Funtion"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)

if __name__ == "__main__":
    etl_web_to_gcs()