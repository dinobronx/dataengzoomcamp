from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df 

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    # returns data/color/yellow_tripdata etc.
    pathdf = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(pathdf, compression="gzip")
    return pathdf

@task()
def write_gcs(pathdf: Path) -> None:
    """Uploading local parquet file to gcs"""
    gcs_block = GcsBucket.load("parquet-demo-gcs")
    gcs_block.upload_from_path(
        from_path=pathdf,
        to_path=pathdf
    )
    return

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url =  f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path_df = write_local(df_clean, color, dataset_file)
    write_gcs(path_df)

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    
    for month in months:
        etl_web_to_gcs(color=color, year=year, month=month)

if __name__ == '__main__':
    #homework
    color = "green"
    months = [1]
    year = 2020
    etl_parent_flow(months, year, color)
 