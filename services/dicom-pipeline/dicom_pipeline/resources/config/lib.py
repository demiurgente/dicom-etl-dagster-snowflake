import os
from dagster import Config, MonthlyPartitionsDefinition

DAGSTER_ENV = os.environ.get("DAGSTER_ENV", "local")
PROJECT_NAME = os.getenv("PROJECT_NAME", "dicom_pipeline")

COLUMN_PARTITION_MAP = {
	'images': "instance_creation_date",
	'series': "study_date",
	'studies': "study_date",
	'staged_images': "instance_creation_date",
	'staged_series': "study_date",
	'staged_studies': "study_date",
	'processed_images': "instance_creation_date",
}

PARTITION_OUT_DATE_FORMAT = "%Y_%m"
PARTITION_MONTHLY = MonthlyPartitionsDefinition(start_date="2004-08", end_date="2004-09", fmt='%Y-%m')
PARTITION_RESTATEMENT = MonthlyPartitionsDefinition(start_date="2000-01", end_date="2023-09", fmt='%Y-%m')
CONCURRENCY_LEVEL = 3

class PartitionConfig(Config):
    column: str
    year: int
    month: int

DICOM_FILE_DIRECTORY = "../../data/dicom-files/"
PYARROW_EXISTING_DATA_BEHAVIOR = 'overwrite_or_ignore' #| overwrite_or_ignore | error | delete_matching
