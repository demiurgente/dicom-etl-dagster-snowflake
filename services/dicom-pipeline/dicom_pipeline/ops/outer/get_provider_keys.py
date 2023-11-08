import json
from typing import Optional
from dagster import (
	MetadataValue,
	OpExecutionContext,
	List,
	String,
	Out,
	op
)
from dagster_aws.s3 import S3FileManagerResource, S3Resource
from ...ops.utils import retrieve_s3_keys
from ...resources.config import PartitionConfig

@op(
	name="get_provider_keys",
    description=(
        "Retrieve the list of files from provider s3 bucket "
        "to be used in subsequent steps to pull image scan data."
    ),
    out={"provider_s3_keys": Out(description="List of s3 keys found in provider bucket")},
)
def s3_get_provider_keys(
    context: OpExecutionContext,
	config: Optional[PartitionConfig],
	s3_provider: S3Resource,
	s3_provider_io: S3FileManagerResource,
	provider_s3_keys: List[String]
) -> List[String]:
    context.log.info(f"Successfully processed provider keys: {provider_s3_keys}")
    target_keys = retrieve_s3_keys(context, config, s3_provider, s3_provider_io)
    context.log.info(f"Found provider files: {target_keys}")
    context.add_output_metadata(
        metadata={
            "num_new_provider_files": len(target_keys),
            "provider_s3_keys": MetadataValue.json(json.dumps({"provider_keys": target_keys}))
        }
    )
    return target_keys
