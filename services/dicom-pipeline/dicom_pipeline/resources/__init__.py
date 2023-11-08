import os
from dagster_aws.s3.resources import S3FileManagerResource, S3Resource
from dagster_aws.s3.io_manager import ConfigurablePickledObjectS3IOManager
from dagster_dbt import dbt_cli_resource
from dagster_duckdb_pandas import DuckDBPandasIOManager, DuckDBPandasTypeHandler
from dagster_duckdb_pyspark import DuckDBPySparkTypeHandler
from dagster_pyspark import PySparkResource
from dagster_slack import SlackResource
from dagster_snowflake import build_snowflake_io_manager
from dagster_snowflake_pandas import SnowflakePandasTypeHandler
from dagster_snowflake_pyspark import SnowflakePySparkTypeHandler
from dagster._utils import file_relative_path
from .config import (
	CONCURRENCY_LEVEL,
	DBT_PROJECT_DIR,
	DBT_DATABASE_PATH,
	DBT_PROFILES_DIR,
	LAMBDA_FUNCTION_NAME,
	SLACK_TOKEN,
	s3_conf_provider_dict,
	s3_conf_raw_dict,
	s3_conf_stage_dict,
	s3_conf_prod_dict,
	snowflake_config,
	spark_conf
)
from .dbt import DbtCli2 as DbtCli
from .directory_resource import TemporaryDirectoryResource
from .duckdb_parquet_io_manager import DuckDBPartitionedParquetIOManager
from .healthchecks import HealthchecksIO
from .lambda_resource import LambdaResource
from .snowflake_io_manager import SnowflakeIOManager
from .parquet_io_manager import S3PartitionedParquetIOManager, LocalPartitionedParquetIOManager
from .multi_db_and_parquet_io_manager import MultiDuckDBAndLocalPartitionedParquetIOManager

# client and manager to copy/sync objects from provider bucket
s3_provider = S3Resource(**s3_conf_provider_dict)
s3_provider_io = S3FileManagerResource(**s3_conf_provider_dict)

# s3 client for raw bucket to save provider files to
s3_raw = S3Resource(**s3_conf_raw_dict)
s3_io_raw = S3FileManagerResource(**s3_conf_raw_dict)
pyspark_conf_raw = spark_conf


# stage layer bucket to put normalized files to
s3_stage = S3Resource(**s3_conf_stage_dict)
s3_io_stage = S3FileManagerResource(**s3_conf_stage_dict)
pyspark_conf_stage = spark_conf

# final layer bucket to put processed files to
s3_prod = S3Resource(**s3_conf_prod_dict)
s3_io_prod = S3FileManagerResource(**s3_conf_prod_dict)
pyspark_configured = PySparkResource(spark_config=spark_conf)

default_resources = {
	"healthchecks": HealthchecksIO.configure_at_launch(),
	"lambda_client": LambdaResource(),
	"pyspark": pyspark_configured,
	"s3_raw": s3_raw,
	"s3_io_raw": s3_io_raw,
	"s3_io_stage": s3_io_stage,
	"s3_provider": s3_provider,
	"s3_provider_io": s3_provider_io,
	"slack": SlackResource(token=SLACK_TOKEN),
	"temp_dir": TemporaryDirectoryResource.configure_at_launch(),
}

RESOURCES_LOCAL = {
    "dbt": dbt_cli_resource.configured({
        "project_dir": DBT_PROJECT_DIR,
		"profiles_dir": DBT_PROFILES_DIR,
        "target": "local",
    }),
	"dbt2": DbtCli(
		project_dir=DBT_PROJECT_DIR,
		profiles_dir=DBT_PROFILES_DIR,
		target="local"
	),
    "dwh_io": DuckDBPartitionedParquetIOManager(duckdb_path=DBT_DATABASE_PATH),
	"io_manager": MultiDuckDBAndLocalPartitionedParquetIOManager(
		database=DBT_DATABASE_PATH,
		pyspark=pyspark_configured
	),
	**default_resources
}

other_resources = []
other_envs = ["stage", "prod"]
s3_other_env_map = {
	"stage": s3_conf_stage_dict,
	"prod": s3_conf_prod_dict
}

for env in other_envs:
    db_key = f'{env.upper()}_DB'
    other_resources.append({
        "dbt": dbt_cli_resource.configured({
            "project_dir": DBT_PROJECT_DIR,
		    "profiles_dir": DBT_PROFILES_DIR,
            "target": env,
        }),
	    "dbt2": DbtCli(
	    	project_dir=DBT_PROJECT_DIR,
	    	profiles_dir=DBT_PROFILES_DIR,
	    	target=env
	    ),
        "dwh_io": SnowflakeIOManager(
            database=db_key,
            **snowflake_config
        ),
	    "io_manager": S3PartitionedParquetIOManager(
	    	**s3_other_env_map[env]
	    ),
	    **default_resources,
    })

RESOURCES_STAGE, RESOURCES_PROD = other_resources
