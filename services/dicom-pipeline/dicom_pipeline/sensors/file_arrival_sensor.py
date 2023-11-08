import pandas as pd
import glob
import os
from dagster_aws.s3.sensor import get_s3_keys
from dagster import (
	DefaultSensorStatus,
	JobDefinition,
	RunConfig,
	RunRequest,
	sensor,
	SensorDefinition,
	SensorEvaluationContext,
	SkipReason,
)
from ..resources.config import (
	COLUMN_PARTITION_MAP,
	PARTITION_MONTHLY,
	s3_conf_provider,
	s3_conf_raw
)
from ..assets.raw import (
	COLUMN_MAP,
	RawConfig
)

def make_scan_s3_provider_bucket_sensor(
    job: JobDefinition,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED
) -> SensorDefinition:
    @sensor(
    	description="Sensor to crawl new dicom file arrivals on the external s3 filesystem",
		minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
		name="provider_data_sensor",
		job=job
	)
    def s3_provider_sensor(
    	context: SensorEvaluationContext,
    ):
        since_key = context.cursor or None
        s3_keys = get_s3_keys(s3_conf_provider.s3_bucket, prefix=s3_conf_provider.s3_prefix, since_key=since_key)
        if not s3_keys:
            yield SkipReason(f"No new files found: s3://{s3_conf_provider.s3_bucket}/{s3_conf_provider.s3_prefix}")
        last_key = s3_keys[-1]
        context.update_cursor(last_key)
        yield RunRequest(
    		job_name=job.name,
    		run_key=last_key,
    		run_config={}
    	)
    return s3_provider_sensor

def make_scan_s3_raw_bucket_sensor(
	job: JobDefinition,
	compress_job: JobDefinition=None,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED
) -> SensorDefinition:
    @sensor(
		description=(
		    "Sensor to kick-off data processing, once raw files are discovered on owned `raw` s3 bucket. "
		   f"Launches run for {list(COLUMN_MAP.keys())} and lambda pixel data compression job (optionally). "
		    "Main gateway for launching processing, once data files arrive from provider source"
		),
		minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
		name=f'raw_data_sensor{f"_with_{compress_job.name}" if compress_job is not None else ""}',
		job=job
	)
    def s3_raw_sensor(
		context: SensorEvaluationContext
	):
        """
    	Sensor to crawl new dicom file arrivals for owned s3 filesystem
    	"""
        since_key = context.cursor or None

        s3_keys = get_s3_keys(s3_conf_raw.s3_bucket, s3_conf_raw.s3_prefix, since_key=since_key)
        if not s3_keys:
            yield SkipReason(f"No new files found for raw bucket resource: s3://{s3_conf_raw.s3_bucket}/{s3_conf_raw.s3_prefix}")
        last_key = s3_keys[-1]
        if compress_job is not None:
            yield RunRequest(
    	    	job_name=compress_job.name,
                run_key=f'compress_pixel_data:{last_key}',
                run_config=RunConfig(ops={
		        	'lambda_compress_pixel_data': {
		        		's3_keys': s3_keys,
		        	},
		        })
            )

        partition_date = PARTITION_MONTHLY.start.date()
        yield RunRequest(
    		job_name=job.name,
    		run_key=last_key,
    		run_config=RunConfig(ops={
			    's3_get_raw_files': {
			    	'config': {
			    		'month': partition_date.month,
			    		'year': partition_date.year
			    	}
			    }
		    })
    	)
        context.update_cursor(last_key)
    return s3_raw_sensor

def make_scan_file_sensor(
	job: JobDefinition,
	directory: str,
	compress_job:JobDefinition=None,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED
) -> SensorDefinition:
    @sensor(
		description=(
		    "Sensor to kick-off data processing, once raw files are discovered on owned `raw` s3 bucket. "
		   f"Launches run for {list(COLUMN_MAP.keys())} and a local pixel data compression job (optionally). "
		    "Main gateway for launching processing, once data files arrive from provider source"
		),
		minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
		name=f'file_sensor_on_{job.name}{f"_with_{compress_job.name}" if compress_job is not None else ""}',
		job=job,
	)
    def my_dir_sensor(context):
        last_mtime = float(context.cursor) if context.cursor else 0
        max_mtime = last_mtime

        dicom_files = []
        exts = ['.dicom', '.dcm']
        for ext in exts:
            dicom_files += glob.glob(f"{directory}*{ext}")
            dicom_files += glob.glob(f"{directory}*/*{ext}")
            dicom_files += glob.glob(f"{directory}*/*/*{ext}")

        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                dicom_files.append(filepath)
                fstats = os.stat(filepath)
                file_mtime = fstats.st_mtime
                if file_mtime <= last_mtime:
                    continue

        context.log.info(f"Collected files: {dicom_files}")
        if len(dicom_files) == 0:
            yield SkipReason(f"No files found in {dir}.")
        context.log.info(f"Launching materialization for: {list(COLUMN_MAP.keys())}")

        if compress_job is not None:
            yield RunRequest(
    	    	job_name=compress_job.name,
                run_key=f'compress_pixel_data:{file_mtime}',
                run_config=RunConfig(ops={
		        	'compress_pixel_data': {
		        		'raw_files': dicom_files,
		        	},
		        })
            )
        run_config = {}
        # launch other assets for dbt / dagster
        for asset, columns in COLUMN_MAP.items():
            raw_config = RawConfig(
                columns=columns,
                asset=asset,
			    files=dicom_files,
    		    partition_column=COLUMN_PARTITION_MAP.get(asset)
            )
            run_config[asset] = raw_config

        context.log.info(raw_config)
        date_range = pd.date_range(
			start=PARTITION_MONTHLY.start.date(),
			end=PARTITION_MONTHLY.end.date(),
			freq='M'
		)
        for date in date_range:
            yield RunRequest(
                run_key = f"raw-assets:{file_mtime}",
                run_config=RunConfig(ops=run_config),
    	    	partition_key=date.strftime("%Y-%m")
            )
        context.update_cursor(str(max(max_mtime, file_mtime)))
    return my_dir_sensor
