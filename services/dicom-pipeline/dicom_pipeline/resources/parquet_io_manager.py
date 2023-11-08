from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.functions import date_format
from typing import Union
import os
import pandas
import pyspark
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
	ConfigurableIOManager,
	DagsterError,
    Field,
    InputContext,
    IOManager,
    OutputContext,
	ResourceDependency,
    _check as check,
)
from dagster_pyspark.resources import PySparkResource
from dagster._seven.temp_dir import get_system_temp_directory
from .config import (
	PARTITION_OUT_DATE_FORMAT,
	PYARROW_EXISTING_DATA_BEHAVIOR,
	PYSPARK_WRITE_MODE
)


class PartitionedParquetIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """
    pyspark: ResourceDependency[PySparkResource]

    @property
    def _base_path(self):
        raise NotImplementedError()

    def handle_output(
        self, context: OutputContext, obj: Union[pandas.DataFrame, PySparkDataFrame]
    ):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        partition_column = context.metadata.get("partition_expr")
        if partition_column is not None:
            if isinstance(obj, pandas.DataFrame):
                row_count = obj.count()
                # format properly to partition folders
                obj['date'] = obj[partition_column].dt.strftime(PARTITION_OUT_DATE_FORMAT)
                table = pa.Table.from_pandas(obj)

				# append/overwrite partitions
                pq.write_to_dataset(
                    table,
                    existing_data_behavior=PYARROW_EXISTING_DATA_BEHAVIOR,
                    root_path=path,
                    partition_cols=['date'],
                )
            elif isinstance(obj, PySparkDataFrame):
                row_count = obj.count()
                obj.withColumn('date', date_format(partition_column, PARTITION_OUT_DATE_FORMAT)) \
				   .write.option("header",True) \
                         .partitionBy(partition_column) \
                         .mode(PYSPARK_WRITE_MODE) \
                         .parquet(path=path)
            else:
                raise DagsterError(f"Outputs of type {type(obj)} not supported.")
        else:
            if isinstance(obj, pandas.DataFrame):
                row_count = len(obj)
                context.log.info(f"Row count: {row_count}")
                if PYSPARK_WRITE_MODE == 'overwrite':
                    obj.to_parquet(path=path, index=False)
                elif PYSPARK_WRITE_MODE == 'append':
                    obj.to_parquet(path=path, index=False, engine='fastparquet', append=True)
                else:
                    raise DagsterError(f"Unexpected type for pyspark write mode: {PYSPARK_WRITE_MODE}")
            elif isinstance(obj, PySparkDataFrame):
                row_count = obj.count()
                obj.write.parquet(path=path, mode=PYSPARK_WRITE_MODE)
            else:
                raise DagsterError(f"Outputs of type {type(obj)} not supported.")

            context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> Union[PySparkDataFrame, str]:
        path = self._get_path(context)
        if context.dagster_type.typing_type == PySparkDataFrame:
            return context.resources.pyspark.spark_session.read.parquet(path)

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        key = context.asset_key.path[-1]
        return os.path.join(self._base_path, f"{key}.parquet")

class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    base_path: str = get_system_temp_directory()

    @property
    def _base_path(self):
        return self.base_path


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    s3_bucket: str
    s3_prefix: str

    @property
    def _base_path(self):
        return "s3://" + self.s3_bucket + '/' + self.s3_prefix
