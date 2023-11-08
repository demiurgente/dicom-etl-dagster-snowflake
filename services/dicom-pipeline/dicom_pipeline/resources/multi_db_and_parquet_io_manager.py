from dagster_snowflake_pandas import SnowflakePandasIOManager
from dagster_duckdb_pandas import DuckDBPandasIOManager
from .parquet_io_manager import (
	LocalPartitionedParquetIOManager,
	S3PartitionedParquetIOManager
)

class MultiDuckDBAndLocalPartitionedParquetIOManager(
	DuckDBPandasIOManager,
	LocalPartitionedParquetIOManager,
):
    """
    Combines `DuckDBPandasIOManager`, `PartitionedParquetIOManager`, writes
    result of pandas calculations to "/tmp" partitioned parquets and pushes
    to duckdb with provided schema prefix. Partition level support is currently
    at month level, definitions can be accessed in underlying managers.
    """
    def handle_output(self, context, obj):
        context.resources.parquet_io.handle_output(context, obj)
        context.resources.dwh_io.handle_output(context, obj)


class MultiSnowflakeAndS3PartitionedParquetIOManager(
	SnowflakePandasIOManager,
	S3PartitionedParquetIOManager
):
    """
    Combines `SnowflakePandasIOManager` and `S3PartitionedParquetIOManager`, writes
    result to <s3 bucket + prefix> and publishes table to Snowflake. Partitions
    data monthly using pre-defined strategies.
    """
    def handle_output(self, context, obj):
        context.resources.parquet_io.handle_output(context, obj)
        context.resources.dwh_io.handle_output(context, obj)
