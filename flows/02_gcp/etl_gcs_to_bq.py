from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """ Download the data from GCS """
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}"
    gcs_block = GcsBucket.load('parquet-demo-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    return Path(gcs_path)

@task()
def transform(gcpath: Path) -> pd.DataFrame:
    """ Data cleaning sample """
    df = pd.read_parquet(gcpath)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """ Write data in Big Query """
    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table='dezoomcampmulti.rides',
        project_id='alien-handler-376020',
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_eq():
    """ Main etl flow to load data to Big Query"""
    color="yellow"
    year=2021
    month=1

    gcpath = extract_from_gcs(color, year, month)
    df = transform(gcpath)
    write_bq(df)

if __name__=='__main__':
    etl_gcs_to_eq()