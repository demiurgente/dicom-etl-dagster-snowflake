from dagster import (
	In,
	Int,
	List,
	OpExecutionContext,
	Out,
	String,
	op
)
from dagster_aws.s3 import S3FileManagerResource, S3Resource

@op(
    name="sync_provider_data",
    description=(
        "Synchronize new provider data from external bucket to "
        "owned s3 bucket. Invoke `aws sync` to perform recursive update"
    ),
	ins={"provider_s3_keys": In(description="List of s3 keys collected from provider bucket")},
    out={"provider_s3_keys": Out(description="List of all historical keys found in external provider bucket")},
)
def s3_sync_provider_data(
    context: OpExecutionContext,
	s3_io_raw: S3FileManagerResource,
	s3_provider_io: S3FileManagerResource,
	provider_s3_keys: List[String]
) -> Int:
    src_url = f's3://{s3_io_raw.s3_bucket}/{s3_io_raw.s3_prefix}'
    dst_url = f's3://{s3_provider_io.s3_bucket}/{s3_provider_io.s3_prefix}'

    cmd_str=f'''aws sync {src_url} {dst_url}'''
    subprocess = subprocess.run(
        cmd_str,
        capture_output=True,
        shell=True
    )

    context.log.info(subprocess.stdout.decode())
    context.log.info(f'Exit code status {subprocess.returncode}')
    return provider_s3_keys
