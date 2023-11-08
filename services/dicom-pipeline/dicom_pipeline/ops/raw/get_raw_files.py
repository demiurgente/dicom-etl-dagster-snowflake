import json
from dagster import (
	In,
	List,
	MetadataValue,
	OpExecutionContext,
	Out,
	String,
	op
)
from dagster_aws.s3 import S3FileManagerResource, S3Resource
from ...ops.utils import retrieve_s3_keys
from ...resources import TemporaryDirectoryResource
from ...resources.config import PartitionConfig

@op(
    name="s3_get_raw_files",
    description=(
        "Retrieve the list of files from owned `raw` layer s3 bucket "
        "to be used in subsequent steps to process scan data."
    ),
	ins={"s3_keys": In(description="List of s3 keys copied to our own bucket")},
    out={"raw_files": Out(description="Pandas DataFrame with files from s3 that were written locally")},
)
def s3_get_raw_files(
    context: OpExecutionContext,
	config: PartitionConfig,
	s3_raw: S3Resource,
	s3_io_raw: S3FileManagerResource,
	temp_dir: TemporaryDirectoryResource,
	s3_keys: List[String]
) -> List[String]:
    """
	Operation to crawl new dicom file arrivals from s3 to local filesystem
	"""
    target_keys = retrieve_s3_keys(context, config, s3_raw, s3_io_raw)
    context.log.info(f"Found provider files: {target_keys}")
    temp_dir.create_dir()
    raw_files = []
    for key in s3_keys:
        context.log.info(f"Downloading {key} from {s3_io_raw.s3_bucket}/{s3_io_raw.s3_prefix}")
        temp_dir.path.joinpath(key).parent.mkdir(parents=True, exist_ok=True)
        file_name = temp_dir.path.joinpath(key)
        raw_files.append(file_name)
        s3_io_raw.download_file(
            Bucket=s3_io_raw.s3_bucket,
            Key=key,
            Filename=file_name,
        )
        context.log.info(f"Writing to {file_name}")
    context.add_output_metadata(
        metadata={
            "num_new_dicom_files": len(raw_files),
            "preview_file_paths": MetadataValue.json(json.dumps({"raw_files": raw_files}))
        }
    )
    return raw_files
