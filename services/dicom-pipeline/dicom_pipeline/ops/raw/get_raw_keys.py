import json
from dagster import (
	In,
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
	name="get_raw_keys",
    description=(
        "Retrieve the list of files from raw s3 bucket after "
		"copying provider files. Those keys will be used in "
		"in subsequent steps to process DICOM data."
    ),
	ins={"provider_s3_keys": In(description="List of s3 keys collected from provider bucket")},
    out={"raw_s3_keys": Out(description="List of s3 keys found in raw bucket after copying provider files")},
)
def s3_get_raw_keys(
    context: OpExecutionContext,
	config: PartitionConfig,
	s3_raw: S3Resource,
	s3_io_raw: S3FileManagerResource,
	provider_s3_keys: List[String]
) -> List[String]:
    context.log.info(f"Successfully processed provider keys: {provider_s3_keys}")
    target_keys = retrieve_s3_keys(context, config, s3_raw, s3_io_raw)
    context.log.info(f"Found provider files: {target_keys}")
    context.add_output_metadata(
        metadata={
            "num_new_s3_keys": len(target_keys),
            "preview_s3_keys": MetadataValue.json(json.dumps({"s3_keys": target_keys}))
        }
    )
    return target_keys
