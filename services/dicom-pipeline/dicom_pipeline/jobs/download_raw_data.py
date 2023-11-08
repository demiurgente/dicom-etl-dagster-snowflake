from dagster import job
from .utils import HOOKS
from ..ops import s3_get_raw_files
from ..resources.config import PARTITION_MONTHLY

@job(
	hooks=HOOKS,
	partitions_def=PARTITION_MONTHLY
)
def aws_s3_download_raw_data_job():
    """
    Download new data from `raw` bucket in s3 locally to process
    """
    s3_get_raw_files()
