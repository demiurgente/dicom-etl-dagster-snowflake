from dagster import job
from .utils import HOOKS
from ..resources.config import PARTITION_MONTHLY
from ..ops import (
	lambda_compress_pixel_data,
	compress_pixel_data,
	s3_get_raw_files,
	s3_get_raw_keys
)

@job(
	hooks=HOOKS,
	partitions_def=PARTITION_MONTHLY
)
def aws_lambda_compress_pixel_data_job():
    """
    Retrieve list of new files from `raw` s3 bucket with modified date from `PARTITION_MONTHLY` range:
    and kick-off aws lambda job that would compress `PixelData` and push resulting images to `stage` layer
    """
    lambda_compress_pixel_data(s3_get_raw_keys())

@job(
	partitions_def=PARTITION_MONTHLY
)
def compress_pixel_data_job():
    """
    Retrieve list of new files from `raw` s3 bucket with modified date from `PARTITION_MONTHLY` range:
    and kick-off aws lambda job that would compress `PixelData` and push resulting images to `stage` layer
    """
    compress_pixel_data(s3_get_raw_files(s3_get_raw_keys()))
