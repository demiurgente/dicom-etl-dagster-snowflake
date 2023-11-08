from dagster import (
	AssetSelection,
	AutoMaterializePolicy,
	define_asset_job,
	graph_asset
)
from dagster_dbt import load_assets_from_dbt_manifest
from .download_raw_data import aws_s3_download_raw_data_job
from .compress_raw_images import (
	aws_lambda_compress_pixel_data_job,
	compress_pixel_data_job
)
from .sync_provider_data import (
	aws_s3_sync_provider_data_to_raw_historical_job,
	aws_s3_sync_provider_data_to_raw_job
)
from ..assets.raw import COLUMN_MAP
from ..assets import OPERATIONAL, PROCESSED, RAW, STAGE
from ..resources.config import PARTITION_MONTHLY, PartitionConfig

operational_assets_job = define_asset_job(
	f"materialize_{OPERATIONAL}_assets_job",
	selection=AssetSelection.groups(OPERATIONAL),
	partitions_def=PARTITION_MONTHLY
)
raw_assets_job = define_asset_job(
	f"materialize_{RAW}_assets_job",
	selection=AssetSelection.groups(RAW),
	partitions_def=PARTITION_MONTHLY
)
stage_assets_job = define_asset_job(
	f"materialize_{STAGE}_assets_job",
	selection=AssetSelection.groups(STAGE),
	partitions_def=PARTITION_MONTHLY
)
processed_assets_job = define_asset_job(
	f"materialize_{PROCESSED}_assets_job",
	selection=AssetSelection.groups(PROCESSED),
	partitions_def=PARTITION_MONTHLY
)
