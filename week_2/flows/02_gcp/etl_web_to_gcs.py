from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web to pandas dataframe"""

    df = pd.read_csv(dataset_url)
    
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix some dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(5))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    data_dir = f'data/{color}'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path.as_posix())

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()