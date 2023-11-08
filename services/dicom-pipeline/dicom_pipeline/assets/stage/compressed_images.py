from pyspark.sql import DataFrame as SparkDF
from matplotlib import pyplot as plt
import pandas as pd
import os
from dagster import (
	AssetExecutionContext,
	AssetIn,
	AutoMaterializePolicy,
	DagsterError,
	List,
	MetadataValue,
	String,
	asset
)
from dagster_aws.s3 import S3FileManagerResource
from ...resources import TemporaryDirectoryResource
from ...resources.config import PARTITION_MONTHLY

@asset(
	ins={"images": AssetIn(key_prefix="raw"),
	     "lambda_image_keys": AssetIn(key_prefix="operational")},
	auto_materialize_policy=AutoMaterializePolicy.eager(),
    partitions_def=PARTITION_MONTHLY,
    compute_kind="pandas",
)
def compressed_images(
	context: AssetExecutionContext,
	s3_io_raw: S3FileManagerResource,
	temp_dir: TemporaryDirectoryResource,
	images: SparkDF,
	lambda_image_keys: List[String],
) -> pd.DataFrame:
    """
    Collect compressed images from s3 resource into pd.DataFrame
    """
    if len(lambda_image_keys) == 0:
        raise DagsterError("Previous function did not produce new images, status on result should be non-null")

    bucket = s3_io_raw.s3_bucket
    prefix = s3_io_raw.s3_prefix
    pulled_files = []

    target_date = pd.to_datetime(context.asset_partition_key_for_output())
    target_part = target_date.strftime("%Y/%m/")

    # unique identifier collected from s3 after lambda function
    # custom merge key is defined in ./services/img-compressor
    s3_keys = target_part + images.series_instance_uid + '|' + images.sop_instance_uid + ".jpeg"

	# download files to temp directory
    for key in s3_keys:
        context.log.info(f"Downloading {key} from {bucket}/{prefix}")
        temp_dir.path.joinpath(key).parent.mkdir(parents=True, exist_ok=True)
        file_name = temp_dir.path.joinpath(key)
        s3_io_raw.download_file(
            Bucket=bucket,
            Key=key,
            Filename=file_name,
        )
        pulled_files.append(file_name)

	# combine downloaded images to dataframe
    images = [ plt.imread(f) for f in pulled_files ]
	# same unique identifier, we split it to columns to perform joins downstream
    files = [ x.split("/")[-1].replace(".jpeg", "") for x in s3_keys ]
    series_uids, sop_uids= map(list, zip(*(s.split('|') for s in files)))
	# clean-up files; images are moved with DataFrame
    for f in pulled_files:
        os.remove(f)
    df = pd.Dataframe({
		"image_data": images,
		"image_filename": files,
		"series_instance_uid": series_uids,
		"sop_instance_uid": sop_uids,
	})

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )
    return df
