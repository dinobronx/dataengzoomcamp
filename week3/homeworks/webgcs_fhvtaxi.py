from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import urllib.request

@task(retries=3)
def fetch(dataset_url: str, filename):
    """download data and save locally"""
    urllib.request.urlretrieve(dataset_url, filename)


@task()
def write_gcs(pathdf: Path) -> None:
    """Uploading local parquet file to gcs"""
    gcs_block = GcsBucket.load("homework-wk4")
    gcs_block.upload_from_path(
        from_path=pathdf,
        to_path=pathdf
    )
    return

@flow()
def etl_web_to_gcs_hw(year: int, month: int) -> None:
    """The main ETL function"""
    fhv_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    fhv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
    
    yellow_file = f"yellow_tripdata_{year}-{month:02}.csv.gz"
    yellow_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02}.csv.gz"
    
    green_file = f"green_tripdata_{year}-{month:02}.csv.gz"
    green_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month:02}.csv.gz"
    fetch(green_url, green_file)
    filename = Path(green_file)
    write_gcs(filename)

    
@flow(log_prints=True)
def etl_gcs_to_eq(months: list[int], year: int):
    """ Main etl flow to load data to Big Query"""

    for month in months:
        etl_web_to_gcs_hw(year, month)
       
        

if __name__=='__main__':
    
    etl_gcs_to_eq([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 2020)