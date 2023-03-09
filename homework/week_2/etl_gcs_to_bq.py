from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return Path(gcs_path)


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-course-378517",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    df_rowcount = len(df.index)
    print(f'Dataframe has {df_rowcount} rows')
    return df_rowcount


@flow()
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    rows_written = write_bq(df)
    print(f'{rows_written} rows appended to BQ')
    return rows_written


@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
) -> None:
    row_count = 0
    for month in months:
        row_count += etl_gcs_to_bq(color, year, month)
    print(f'Total of {row_count} rows appended to BQ')


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_parent_flow(color, year, months)
