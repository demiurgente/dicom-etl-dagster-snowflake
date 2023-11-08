from dagster import (
	AutoMaterializePolicy,
	List,
	String,
	graph_asset
)
from ...ops import lambda_compress_pixel_data

@graph_asset(
	description = "Execute lambda function to compress images and deliver status response",
	auto_materialize_policy=AutoMaterializePolicy.eager(),
	key = "lambda_image_keys"
)
def lambda_image_keys(
	raw_files: List[String]
):
    return lambda_compress_pixel_data(raw_files)
