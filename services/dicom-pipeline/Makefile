DBT := dbt_snowflake

export DBT_DATABASE_NAME := dicom.duckdb


.PHONY: all
all:
  echo DBT_DATABASE_NAME: $$DBT_DATABASE_NAME


dagster_deps:
	pip install -e ".[dev]"

dbt_deps:
	dbt deps --project-dir=${DBT} --profiles-dir=${DBT}/config
	cd ${DBT} && dbt seed --project-dir=. --profiles-dir=config && cd ..
