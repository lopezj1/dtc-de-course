from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task 

color = "yellow"
year = 2019
month = 2
dataset_file = f"{color}_tripdata_{year}-{month:02}"

data_dir = f"data/{color}"
Path(data_dir).mkdir(parents=True, exist_ok=True)
path = Path(f"{data_dir}/{dataset_file}.parquet")


@task(log_prints=True)
def upload_to_gcs():
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    print(gcp_cloud_storage_bucket_block.list_folders())

    bucket_name = gcp_cloud_storage_bucket_block.get_bucket().name
    from_path = path
    to_path = Path("tmp", path)
    
    if from_path.is_file():    
        print(f"Source file found on '{from_path}'...")
        print(f"Preparing to upload to: 'gcs://{bucket_name}/{to_path}'...")
        gcp_cloud_storage_bucket_block.upload_from_path(from_path=path.as_posix(), 
                                                        to_path=to_path.as_posix())
    else:
        print("Source file not found. Aborting...")


@flow(log_prints=True)
def main():
    upload_to_gcs()

if __name__ == "__main__":
    main()