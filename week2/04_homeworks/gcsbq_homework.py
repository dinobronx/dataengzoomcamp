from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@flow(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """ Download the data from GCS """
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}"
    gcs_block = GcsBucket.load('parquet-demo-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    
    df = pd.read_parquet(gcs_path)
    write_bq(df)

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """ Write data in Big Query """
    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table='dezoomhomework.rides',
        project_id='alien-handler-376020',
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_eq(months: list[int], color: str, year: int):
    """ Main etl flow to load data to Big Query"""

    for month in months:
        extract_from_gcs(color, year, month)
       
        

if __name__=='__main__':
    etl_gcs_to_eq([2, 3], 'yellow', 2019)