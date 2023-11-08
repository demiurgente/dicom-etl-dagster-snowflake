import json
import subprocess
from dagster import (
    In,
	Int,
	OpExecutionContext,
	Out,
	List,
	String,
	op
)
from dagster_aws.s3 import S3FileManagerResource
from ...resources.lambda_resource import LambdaResource, LambdaConfig

@op(
    name="lambda_compress_pixel_data",
    description=(
        "Launch aws lambda function to compress scan data from filescans arrived to "
        "s3 bucket. Images will be stored as JPEG for further use in ML tasks."
    ),
	ins={"s3_keys": In(description="List of raw dicom keys on s3")},
    out={"lambda_status": Out(description="Number of converted images as response from a lambda function")},
)
def lambda_compress_pixel_data(
    context: OpExecutionContext,
	config: LambdaConfig,
	lambda_client: LambdaResource,
	s3_io_raw: S3FileManagerResource,
	s3_io_stage: S3FileManagerResource,
    s3_keys: List[String],
) -> Int:
    """
    Launch a cloud lambda function to compress raw pixel data and push resulting image to S3. 
	Implementation can be found in `./services/img-compressor/`;
	"""
    context.log.info(f"Submitting lambda request for: {config.function_name} - {config.qualifier} for file: {s3_keys}")
    payload = {
		# source s3 with raw dicom files
		'source_bucket': s3_io_raw.s3_bucket,
		'source_prefix': s3_io_raw.s3_prefix,
		# destination s3 details to write compressed images to
		'target_bucket': s3_io_stage.s3_bucket,
		'target_prefix': s3_io_stage.s3_prefix,
		# list of dicom files from source bucket to process
		'files': s3_keys
	}
    response = lambda_client.invoke(
		FunctionName=config.function_name,
		InvocationType=config.invocation_type,
		Payload=json.dumps(payload),
		qualifier=config.qualifier
	)
    return response["result"]

@op(
    name="compress_pixel_data",
    description=(
        "Launch a subprocess to compress scan data from local DICOM scans. "
        "s3 bucket. Images will be stored as JPEG for further use in ML tasks."
	    "Binary will compress images and push them to S3. Implementation details"
		"Can be found in crate `./services/img-compressor/`;"
    ),
	ins={"raw_files": In(description="List of raw dicom file names stored locally to process")},
    out={"status": Out(description="Number of converted images as response from a binary run")},
)
def compress_pixel_data(
    context: OpExecutionContext,
	s3_io_raw: S3FileManagerResource,
	s3_io_stage: S3FileManagerResource,
	raw_files: List[String],
) -> Int:
    _compress_config = {
		# source s3 with raw dicom files
		'source_bucket': s3_io_raw.s3_bucket,
		'source_prefix': s3_io_raw.s3_prefix,
		# destination s3 details to write compressed images to
		'target_bucket': s3_io_stage.s3_bucket,
		'target_prefix': s3_io_stage.s3_prefix,
	}
    status = 0
    offset = 0
    FILE_LIMIT = 2500
	# bash has limit for arguments defined by ARGMAX
    # limit number of files provided to binary to not overflow
    args =  [x.split("/")[-1] for x in raw_files]
    for i in range(0, len(args), FILE_LIMIT-1):
        if i % 2500:
            cmd_str='img-compressor '
            for filename in args[offset:offset + FILE_LIMIT]:
                cmd_str += filename + ' '
            sb = subprocess.run(
                cmd_str,
                capture_output=True,
                shell=True
            )
            context.log.info(sb)
            offset += FILE_LIMIT
            if sb.returncode == 0:
                status += FILE_LIMIT

    return status
