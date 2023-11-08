from dagster import (
	In,
	OpExecutionContext,
	Out,
	List,
	String,
	op
)
from dagster_aws.s3 import S3FileManagerResource, S3Resource

@op(
    name="copy_provider_data",
    description=(
        "Copy the list of files from provider s3 bucket "
        "to be used in subsequent steps to pull image scan data"
    ),
	ins={"provider_s3_keys": In(description="List of s3 keys collected from provider bucket")},
    out={"s3_keys": Out(description="List of s3 keys from own raw bucket after copying provider files filtered by partition")},
)
def s3_copy_provider_data(
    context: OpExecutionContext,
	s3_provider: S3Resource,
	s3_provider_io: S3FileManagerResource,
	s3_io_raw: S3FileManagerResource,
	provider_s3_keys: List[String]
) -> List[String]:
    copy_sources = [{ 'Bucket': s3_provider_io.s3_bucket,
		              'Key': key } for key in provider_s3_keys]

    s3_keys = []
	# copy each file to own bucket
    for source in copy_sources:
        # we preserve source key of unknown form a/b/c/key.dicom; change later, once relevant
        s3_keys.append(source['Key'])
        s3_provider.get_client().meta.client.copy(source, s3_io_raw.s3_bucket, source['Key'])
        context.log.info(f"Successful transfer for s3://{s3_io_raw.s3_bucket}/{source['Key']}")
    return s3_keys
