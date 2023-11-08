from dateutil import parser
from pathlib import Path
from typing import Any, Generator, Mapping, Union
import textwrap
import json
from dagster import (
	AssetCheckResult,
	AssetExecutionContext,
	AssetKey,
	AutoMaterializeRule,
	AutoMaterializePolicy,
	AssetObservation,
	BackfillPolicy,
	DagsterError,
	OpExecutionContext,
	Optional,
	Output,
	load_assets_from_package_module
)
from dagster_dbt import (
    DagsterDbtTranslator,
	DbtCliEventMessage,
    DbtCliInvocation,
    DbtCliResource,
    dbt_assets,
	default_metadata_from_dbt_resource_props,
	load_assets_from_dbt_project
)
from dagster._utils import file_relative_path
from dagster_cloud.dagster_insights import dbt_with_snowflake_insights
from . import raw, stage, processed, operational
from ..resources.config import (
    COLUMN_PARTITION_MAP,
	DBT_PROJECT_NAME,
	DBT_PROFILES_DIR,
	PARTITION_MONTHLY
)

DBT_PROJECT_PATH = f"../../{DBT_PROJECT_NAME}"

DBT_PROJECT_DIR = Path(__file__).joinpath("..", DBT_PROJECT_PATH).resolve()
DBT_MANIFEST = Path(file_relative_path(__file__, f"{DBT_PROJECT_PATH}/target/manifest.json"))
DBT_PROFILES_DIR = file_relative_path(__file__, f"../../{DBT_PROFILES_DIR}")

RAW = "raw"
raw_assets = load_assets_from_package_module(raw, group_name=RAW, key_prefix=[RAW])

STAGE = "stage"
stage_assets = load_assets_from_package_module(stage, group_name=STAGE, key_prefix=[STAGE])

PROCESSED = "processed"
processed_assets = load_assets_from_package_module(processed, group_name=PROCESSED, key_prefix=[PROCESSED])

OPERATIONAL='operational'
operational_assets = load_assets_from_package_module(operational, group_name=OPERATIONAL, key_prefix=[OPERATIONAL])

allow_outdated_parents_policy = AutoMaterializePolicy.eager().without_rules(
    AutoMaterializeRule.skip_on_parent_outdated()
)

# identify tables that will be populated by dbt
INTERNAL_TABLES = ["anonymization_mapping","surrogate_key_mapping","salt"]
DATABASE_TABLES = INTERNAL_TABLES + [
    "staged_devices","staged_patients",
	"processed_devices","processed_patients"
]
DATABASE_PARTITIONED_TABLES = [
	"staged_series","staged_studies","staged_images",
	"processed_series","processed_studies"
]
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_description(self, dbt_resource_props: Mapping[str, Any]) -> str:
        description = f"dbt model for: {dbt_resource_props['name']} \n \n"
        return description + textwrap.indent(
            dbt_resource_props.get("raw_code", ""), "\t"
        )

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]):
        node_path = dbt_resource_props["path"]
        prefix = node_path.split("/")[0]
        if "processed/" in node_path:
            prefix = PROCESSED
        elif "stage/" in node_path:
            prefix = STAGE
        elif "models/" in node_path:
            prefix = RAW
        else:
            prefix = "internal"
        return prefix

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        prefix = self.get_group_name(dbt_resource_props)
        return AssetKey([prefix, dbt_resource_props["name"]])

    def get_auto_materialize_policy(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[AutoMaterializePolicy]:
        return allow_outdated_parents_policy

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = {}
        name = dbt_resource_props["name"]
        if COLUMN_PARTITION_MAP.get(name) is not None:
            if name in DATABASE_PARTITIONED_TABLES:
                metadata = {"partition_expr": COLUMN_PARTITION_MAP[name]}

        default_metadata = default_metadata_from_dbt_resource_props(dbt_resource_props)

        return {**default_metadata, **metadata}

def _process_partitioned_dbt_assets(context: OpExecutionContext, dbt2: DbtCliResource):
    # map partition key range to dbt vars
    first_partition, last_partition = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )
    dbt_vars = {"min_date": str(first_partition), "max_date": str(last_partition)}
    dbt_args = ["run", "--vars", json.dumps(dbt_vars)]

    dbt_cli_task = dbt2.cli(dbt_args, context=context)

    # This function adds model start and end time to derive total execution time
    def handle_dbt_event(event: DbtCliEventMessage) -> Generator[Union[Output, AssetObservation, AssetCheckResult], None, None]:
        for dagster_event in event.to_default_asset_events(
            manifest=dbt_cli_task.manifest
        ):
            if isinstance(dagster_event, Output):
                event_node_info = event.raw_event["data"]["node_info"]

                started_at = parser.isoparse(event_node_info["node_started_at"])
                completed_at = parser.isoparse(event_node_info["node_finished_at"])
                metadata = {
                    "Execution Started At": started_at.isoformat(timespec="seconds"),
                    "Execution Completed At": completed_at.isoformat(timespec="seconds"),
                    "Execution Duration": (completed_at - started_at).total_seconds(),
                }

                context.add_output_metadata(
                    metadata=metadata,
                    output_name=dagster_event.output_name,
                )
            yield dagster_event
    # This function emits an AssetObservation with the dbt model's invocation ID and unique ID (needed for Snowflake Insights)
    def handle_all_dbt_events(dbt_cli_task: DbtCliInvocation) -> Generator[Union[Output, AssetObservation, AssetCheckResult], None, None]:
        for raw_event in dbt_cli_task.stream_raw_events():
            yield from handle_dbt_event(raw_event)

    yield from dbt_with_snowflake_insights(context, dbt_cli_task, dagster_events=handle_all_dbt_events(dbt_cli_task))

    if not dbt_cli_task.is_successful():
        raise DagsterError("dbt command failed, see preceding events")

@dbt_assets(
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    backfill_policy=BackfillPolicy.single_run(),
    select=' '.join(DATABASE_PARTITIONED_TABLES),
    partitions_def=PARTITION_MONTHLY,
	io_manager_key="dwh_io",
    manifest=DBT_MANIFEST,
)
def dbt_monthly_assets(context: OpExecutionContext, dbt2: DbtCliResource):
    yield from _process_partitioned_dbt_assets(context=context, dbt2=dbt2)

@dbt_assets(
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    backfill_policy=BackfillPolicy.single_run(),
    select=' '.join(DATABASE_TABLES),
	io_manager_key="dwh_io",
    manifest=DBT_MANIFEST,
)
def dbt_any_assets(context: OpExecutionContext, dbt2: DbtCliResource):
    context.log.info(context)
    yield from dbt2.cli(["build"], context=context).stream()
