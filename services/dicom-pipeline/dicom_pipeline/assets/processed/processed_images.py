import pandas as pd
from dagster import (
	AssetExecutionContext,
	AssetIn,
	AutoMaterializePolicy,
	asset
)
from ...resources.config import (
	COLUMN_PARTITION_MAP,
	PARTITION_MONTHLY
)

@asset(
	metadata={"partition_expr": COLUMN_PARTITION_MAP["processed_images"]},
	auto_materialize_policy=AutoMaterializePolicy.eager(),
	ins={
		"compressed_images": AssetIn(key_prefix="stage"), 
	    "staged_images": AssetIn(key_prefix="stage")
	},
    partitions_def=PARTITION_MONTHLY,
	compute_kind="pandas",
)
def processed_images(
	context: AssetExecutionContext,
	compressed_images: pd.DataFrame,
	staged_images: pd.DataFrame,
)-> pd.DataFrame:
    """
	Combines dataframe of compressed images with staging images normalized table
    """
    df = pd.merge(staged_images, compressed_images, on=['series_instance_uid', 'sop_instance_uid'])
	# partition to target month
    target_date = pd.to_datetime(context.asset_partition_key_for_output())
    df.instance_creation_date = pd.to_datetime(df.instance_creation_date, format="%Y%m%d")
    df = df[(df.instance_creation_date.dt.year == target_date.year) &
            (df.instance_creation_date.dt.month == target_date.month)]
    return compressed_images.join(staged_images)
