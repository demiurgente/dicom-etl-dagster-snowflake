from dagster import graph_asset
from ...ops import (
	s3_get_provider_keys,
	s3_copy_provider_data,
    s3_get_raw_files
)

@graph_asset(
	description = (
		"Retrieve dicom scan data from external source s3 bucket upon arrival "
		"and insert it to own bucket. Launch jobs to normalise sources, "
		"compress scan images and push result to Snowflake"
	),
	key = "raw_files"
)
def raw_files():
    return s3_get_raw_files(s3_copy_provider_data(s3_get_provider_keys()))
