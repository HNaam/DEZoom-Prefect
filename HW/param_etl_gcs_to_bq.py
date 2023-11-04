from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials



@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """"Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../../datafromgcs/")
    return Path(f"../../datafromgcs/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df['passenger_count'].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-403021",
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
@flow(log_prints=True)
def etl_gcs_to_bq(month: int, year: int, color: str, write: bool = False) -> int:
    """ Main ETL flow to load data into BigQuery"""
    path = extract_from_gcs(color,year,month)
    df = transform(path)
    len(df)
    if write == True:
        write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq_parent(months: list[int], year: int , color: str, write : bool):
    rowsprocessed = 0
    for month in months:
        rows = etl_gcs_to_bq(month, year, color, write)
        rowsprocessed += rows
    print(f"Total rows processed: {rowsprocessed}")
if __name__=="__main__":

    months = [2,3]
    year = 2019
    color = "yellow"
    etl_gcs_to_bq_parent(months, year, color)