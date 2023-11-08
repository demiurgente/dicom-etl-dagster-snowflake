# Description

The crate contains the main logic for processing the flow of data from provider to destination schemas in **Snowflake**. The amount of code is fairly small and can be covered quickly. The goal was to set up a system that would be easy to port for own projects and reuse without having to browse the documentation. The code will be changing in the future to address some issues as well as to perform a more thorough set of tests in the cloud.

# Project Structure

# Core Modules

```
└── dicom_pipeline
    ├── assets
    ├── jobs
    ├── ops
    ├── resources
    └── sensors
```

The current organization of code decouples modules into core abstractions that **Dagster** has designed. Those abstractions represent assets (anything can be materialized), operations (ops), jobs (set of instructions on how to execute operations consequently), resources (IO configurable source similar to JDBC), and sensors (list of instructions to periodically execute jobs). Those abstractions are fairly convenient as they offer a controlled level of customization over different aspects of data processing. Below will be presented general idea for each of them. The main entry point for declaring the pipeline is [**init**.py](./__init__.py) in the main folder. It acquires definitions of those abstractions into a run configuration to launch a web server in a similar manner:

```
defs = Definitions(
    assets=assets,
    jobs=jobs,
    sensors=sensors,
    resources=resources
)

```

For development it is advised to use [docker-compose](https://github.com/dagster-io/dagster/blob/1.5.6/examples/deploy_docker/docker-compose.yml) as internally **Dagster** uses grpc, which allows to have a server running in docker and inject user code while testing. The main benefit is that you will not have to restart your server every time your code has changed and will load it into a running instance instead.

# Assets

This is the abstraction for data output, similar to XCOM in **Airflow** (it supports only json/dict), but with more flexibility as you can declare how to handle asset's partitions (split data based on a rule and reduce memory footprint), how to materialize your data (one parent updated, all of them) and types are user declared as long as you have a resource interface support it; I mostly used lists, ints, dataframes (Pandas, PySpark). In terms of code organization, I tried to separate data processing assets into the following categories:

```
└── dicom_pipeline
    └── assets
        ├── operational
        ├── processed
        ├── raw
        └── stage
```

`operational` is used for passing temporary data that changes frequently (i.e. file paths from **S3** resource), they are extensively used to let operations know which files to download or copy to the other bucket. As for `raw` module, that is the core module where initial data is mapped onto different tables. I have already mentioned the details of identifying **DICOM** entities [here](../../../README.md#L21-L47), idea was to understand the process behind data and introduce code that would pick only a subset of columns that correspond to the entity, say `device`. After selecting those columns, data is read using a common `pydicom` reader interface, converted to a `pd.DataFrame`, loaded to the database and persisted on **S3** for long-term use. The next module `stage` represents transformations that should be persisted in the intermediate layer, it will be used as a building block to create a `processed` table; For example, to produce `processed_images` table for this project we are required to collect compressed JPEG images from **S3** and combine them with `staged_images` table that contains measurements, orientation details; Both tables reside in `stage` and an asset declared in `processed` folder will join them an upload both to **S3**and to the database of our choice.

# Ops

Operations is similar to a function and **Airflow** mostly uses operations as points in their DAGs. There is a lot of confusion regarding the proper use of assets and operations as **Dagster** makes a distinct separation between them and people usually opt out to one of them as the main entity to control the data flow. In this project, I have decided to focus mainly on assets to represent specific materialization stages. As for operations, I used them to construct procedures such as downloading keys from a specific start time, checking if provider files arrived, or launching a local subprocess to compress images, essentially I do not want to materialize any assets after those operations, they would be produced as a side effect or passed down to a function that does not materialize them either.

```
└── ops
    ├── outer
    └── raw
```

There are two modules declared: `outer` represents interaction with resources outside the scope of our project to list files on the provider's storage and copy them to our `raw` storage. `raw` offers pretty much the same functionality, get files that came in recently and download them locally to start processing, another one, is to compress `PixelData` that currently is done in two modes: **AWS Lambda**, so spin-off a remote job that would crawl **S3** for new files, process them and upload back and another is local `compress_pixel_data`, it will use the same [software](../../img-compressor/README.md) to compress images but will run on a local machine instead of a paid service.

# Jobs

Job is the main abstraction to execute multiple operations or establish a chain of consequent transformations under a single flow. Mostly, they are composed of operations from the previous steps, like the following:

```python
compress_pixel_data(s3_get_raw_files(s3_get_raw_keys()))
```

Which would call corresponding ops in the following order:

- obtain names of files for the specified date (`s3_get_raw_keys`)
- download files with the above names locally from **S3** (`s3_get_raw_files`)
- execute a binary to compress raw pixel data into a JPEG image locally (`compress_pixe_data`)

Other jobs follow a similar pattern and can be located in the directory `jobs`. Another way of declaring jobs without using operations is by selecting assets (tables) from **Dagster** that we would like to materialize, I mostly used this way of declaration for core module tables that reside in the `assets` folder. The declaration, where the group is a list of definitions from the module with folder `assets/stage` looks like this:

```python
stage_assets_job = define_asset_job(
	f"materialize_stagE_assets_job",
	selection=AssetSelection.groups(STAGE),
)
```

# Resources

The resource is a client for interacting with APIs, databases, filesystem and other custom entities that relate to your use case. The approach is to initialize them once and share them across resources to have more control over side effects. The main benefit is that you can set up an environment-dependent configuration that would change once you switch the target `local->prod` without supporting the configuration in jobs or ops. Basically, your workflow will stay unchanged and you do not have to control for environment in your code as it would be delegated to resource configuration. For example, locally, I used a **DuckDB** for prototyping **Snowflake** as well as a local parquet writer, which would change automatically to **S3** once environment variable `DAGSTER_ENV` is switched to `prod`.

# Sensors

For sensors I have focused only on things that are required by data processing flow, their nature is to have a custom polling mechanic over time that would check for some condition to be met to execute some job. In my case, one of the routines is to await new files that arrived to **S3** and to schedule a full update for `assets` once those files are downloaded. Configurations that are present support **AWS Lambda** as well to capture when the remote function has finished and to launch a job of building compressed images and data related to them.
