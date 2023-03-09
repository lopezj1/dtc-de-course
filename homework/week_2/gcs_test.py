from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket

# color = "yellow"
# year = 2019
# month = 2
color = "green"
year = 2020
month = 1
dataset_file = f"{color}_tripdata_{year}-{month:02}"

data_dir = f"data/{color}"
Path(data_dir).mkdir(parents=True, exist_ok=True)
path = Path(f"{data_dir}/{dataset_file}.parquet")
path = path.as_posix()

gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
print(gcp_cloud_storage_bucket_block.list_folders())
gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
