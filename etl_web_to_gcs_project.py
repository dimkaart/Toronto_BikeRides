import pandas as pd
from zipfile import ZipFile
import json

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task()
def fetch_2016(webpage_url:str, file_structure:dict, column_names: list[str]):
    pass

@task()
def fatch_2017(webpage_url:str, file_structure:dict, column_names: list[str]):
    pass


@task()
def parse_json_file(json_file:str):
    with open(json_file, 'r') as f:
        data_structure = json.load(f)
    return data_structure


@flow()
def etl_parent_flow():
    json_file = './structure_opentoronto.json'
    
    data_structure = parse_json_file(json_file)
    
    df_2016 = fetch_2016(webpage_url = data_structure["2016"]["url"],
                        file_structure=data_structure["2016"]["Structure"],
                        column_names=data_structure["2016"]["Column Names"])
    
    
    
    
###
#@task(retries=3)
#def fetch(dataset_url: str) -> pd.DataFrame:
#    """Read taxi data from web into pandas DataFrame"""
#    df = pd.read_csv(dataset_url)
#    return df
#
#
#@task(log_prints=True)
#def clean_yellow(df=pd.DataFrame) -> pd.DataFrame:
#    """Fix dtype issues"""
#    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
#    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
#    print(df.head(2))
#    print(f"columns: {df.dtypes}")
#    print(f"rows: {len(df)}")
#    return df
#
#
#@task(log_prints=True)
#def clean_green(df=pd.DataFrame) -> pd.DataFrame:
#    """Fix dtype issues"""
#    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
#    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
#    print(df.head(2))
#    print(f"columns: {df.dtypes}")
#    print(f"rows: {len(df)}")
#    return df
#
#
#@task(log_prints=True)
#def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
#    """Write DataFrame out locallz as parquet file"""
#    path = Path(f"data/{color}/{dataset_file}.parquet")
#    df.to_parquet(path, compression="gzip")
#    return path
#
#
#@task(log_prints=True)
#def write_gcs(path: Path) -> None:
#    """Upload local parquet file to GCS"""
#    gcs_block = GcsBucket.load("dtc-dez-gcs")
#    gcs_block.upload_from_path(from_path=path, to_path=path)
#    return
#
#
#@flow()
#def etl_web_to_gcs(color: str, year: int, month: int) -> None:
#    """The main ETL function"""
#    dataset_file = f"{color}_tripdata_{year}-{month:02}"
#    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
#
#    df = fetch(dataset_url)
#    if color == "yellow":
#        df_clean = clean_yellow(df)
#    elif color == "green":
#        df_clean = clean_green(df)
#    path = write_local(df_clean, color, dataset_file)
#    write_gcs(path)
#
#
#@flow()
#def etl_parent_flow(
#    color: str = "yellow", year: int = 2021, months: list[int] = [1, 2, 3]
#):
#    for month in months:
#        etl_web_to_gcs(color, year, month)
#
#
#if __name__ == "__main__":
#    color = "yellow"
#    year = 2021
#    months = [1, 2, 3, 4, 5, 6, 7]
#
#    etl_parent_flow(color, year, months)
#