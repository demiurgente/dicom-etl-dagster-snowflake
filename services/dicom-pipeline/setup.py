from setuptools import find_packages, setup

import glob

setup(
    name="dicom_pipeline",
    packages=find_packages(exclude=[f"dicom_pipeline_tests"]),
    package_data={
        "dicom_pipeline": ["../" + path for path in glob.glob(f"dbt_snowflake/**", recursive=True)]
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "dagster-duckdb-pyspark",
        "dagster-pyspark",
		"dagster-slack",
        "dagster-snowflake-pandas",
        "dagster-snowflake-pyspark",
		"dbt-core",
        "dbt-duckdb",
		"dbt-snowflake",
		"duckdb!=0.3.3, <= 6.0.0",
        "Faker==18.4.0",
		"httpx",
		"inflection",
        "matplotlib",
        "pandas",
        "pyarrow>=4.0.0",
		"pydantic_vault",
        "pydicom",
        "pyspark",
        "numpy",
        "requests",
		"sqlalchemy<2.0.0",
		"snowflake-connector-python",
        "snowflake-sqlalchemy",
    ],
    extras_require={"dev": ["dagster-webserver", "dagster-graphql", "pytest"]},
)
