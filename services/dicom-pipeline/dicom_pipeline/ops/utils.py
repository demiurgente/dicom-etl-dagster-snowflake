from typing import Optional
from dagster import List, OpExecutionContext, String
from dagster_aws.s3 import S3FileManagerResource, S3Resource
from ..resources.config import PartitionConfig, S3_MAX_LIST_KEYS

def is_partition_on_s3(
	x,
	config: Optional[PartitionConfig]
) -> bool:
    if PartitionConfig is None:
        return True
    return (x["LastModified"].year == config.year) & \
           (x["LastModified"].month == config.month)

def retrieve_s3_keys(
	context: OpExecutionContext,
	config: Optional[PartitionConfig],
	s3_client: S3Resource,
	s3_io_manager: S3FileManagerResource
) -> List[String]:
    """
	Common function to list objects for specific `s3_io_manager` and 
	filter keys by modified date from range in `PartitionConfig`
	"""
    cursor = ''
    contents = []

    while True:
        response = s3_client.list_objects_v2(
            Bucket=s3_io_manager.s3_bucket,
            Delimiter="",
            MaxKeys=S3_MAX_LIST_KEYS,
            Prefix=s3_io_manager.s3_prefix,
            StartAfter=cursor,
        )
        contents.extend(response.get("Contents", []))
        if response["KeyCount"] < S3_MAX_LIST_KEYS:
            break

        cursor = response["Contents"][-1]["Key"]

    key_mask = [x["LastModified"] for x in contents if is_partition_on_s3(x, config)]
    target_keys = [obj["Key"] for obj in contents[key_mask]]

    context.log.info(f"Found provider files: {target_keys}")
    return target_keys
