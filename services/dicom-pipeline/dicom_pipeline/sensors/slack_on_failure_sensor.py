from dagster import JobDefinition, SensorDefinition
from dagster_slack import make_slack_on_run_failure_sensor
from ..resources.config import (
	SLACK_NOTIFY_JOB_MAP,
	SLACK_TOKEN
)

def make_slack_on_failure_sensor(
	base_url: str,
	job: JobDefinition
) -> SensorDefinition:
    return make_slack_on_run_failure_sensor(
        channel=SLACK_NOTIFY_JOB_MAP[job.name],
        slack_token=SLACK_TOKEN,
        webserver_base_url=base_url,
    )
