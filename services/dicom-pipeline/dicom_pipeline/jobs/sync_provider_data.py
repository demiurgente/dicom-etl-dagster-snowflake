from dagster import job
from .utils import HOOKS
from ..resources.config import (
	PARTITION_RESTATEMENT,
	PARTITION_MONTHLY
)
from ..ops import (
	s3_copy_provider_data,
	s3_get_provider_keys,
    s3_sync_provider_data
)

@job(
	hooks=HOOKS,
    partitions_def=PARTITION_MONTHLY
)
def aws_s3_sync_provider_data_to_raw_job():
    """
    Launch a process to copy new files from provider s3 bucket filtered by `PARTITION_MONTHLY` range;
    Used to obtain s3 keys that should be processed for specific <Month, Day, ..> partition
    level; i.e. copy last month data from provider to owned raw bucket.
    """
    s3_copy_provider_data(s3_get_provider_keys())

@job(
	hooks=HOOKS,
    partitions_def=PARTITION_RESTATEMENT
)
def aws_s3_sync_provider_data_to_raw_historical_job():
    """
    Synchronize provider entire history of provider files to owned raw bucket; Append fies
    that do not exist in owned raw bucket, do full synchronisation from source. Does
    not overwrite existing keys. Useful for restating on clean buckets, time range;
	i.e. couple of years can be modified by `PARTITION_RESTATEMENT`.
    """
    s3_sync_provider_data(s3_get_provider_keys())
