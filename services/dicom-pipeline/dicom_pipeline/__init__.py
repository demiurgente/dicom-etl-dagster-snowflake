import os

from dagster import Definitions, DefaultSensorStatus, multiprocess_executor

from .assets import (
	dbt_any_assets,
	dbt_monthly_assets,
	operational_assets,
	processed_assets,
	raw_assets,
	stage_assets,
)
from .jobs import (
	aws_lambda_compress_pixel_data_job,
	aws_s3_download_raw_data_job,
	aws_s3_sync_provider_data_to_raw_historical_job,
	aws_s3_sync_provider_data_to_raw_job,
	compress_pixel_data_job,
	operational_assets_job,
	processed_assets_job,
	raw_assets_job,
	stage_assets_job,
)
from .sensors import (
	make_scan_file_sensor,
	make_scan_lambda_sensor,
	make_scan_s3_provider_sensor,
	make_scan_s3_raw_sensor,
	make_slack_on_failure_sensor
)
from .resources import (
	CONCURRENCY_LEVEL,
	RESOURCES_LOCAL,
	RESOURCES_STAGE,
	RESOURCES_PROD
)
from .resources.config import (
	DAGSTER_ENV,
	DICOM_FILE_DIRECTORY,
	PROJECT_NAME
)

assets = [
	dbt_any_assets,
	dbt_monthly_assets,
	*operational_assets,
	*processed_assets,
	*raw_assets,
	*stage_assets,
]

jobs = [
	aws_lambda_compress_pixel_data_job,
	aws_s3_download_raw_data_job,
	aws_s3_sync_provider_data_to_raw_historical_job,
	aws_s3_sync_provider_data_to_raw_job,
	compress_pixel_data_job,
	operational_assets_job,
	processed_assets_job,
	raw_assets_job,
	stage_assets_job,
]

resources_by_deployment_name = {
    "local": RESOURCES_LOCAL,
	"prod": RESOURCES_PROD,
	"stage": RESOURCES_STAGE
}

SENSOR_MODE=DefaultSensorStatus.STOPPED
if DAGSTER_ENV in ["prod", "stage"]:
    SENSOR_MODE=DefaultSensorStatus.RUNNING

new_s3_provider_files = make_scan_s3_provider_sensor(
	aws_s3_sync_provider_data_to_raw_job,
	default_status=SENSOR_MODE
)
raw_tables_on_file = make_scan_file_sensor(
	raw_assets_job,
	DICOM_FILE_DIRECTORY,
	default_status=SENSOR_MODE
)
sensors = [
	new_s3_provider_files,
	raw_tables_on_file,
]


defs = Definitions(
	executor=multiprocess_executor.configured({"max_concurrent": CONCURRENCY_LEVEL}),
    assets=assets,
    jobs=jobs,
    sensors=sensors,
    resources=resources_by_deployment_name[DAGSTER_ENV]
)
