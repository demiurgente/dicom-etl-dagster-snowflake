from dagster import (
	DefaultSensorStatus,
	JobDefinition,
	RunRequest,
	sensor,
	SensorDefinition,
	SensorEvaluationContext,
	SkipReason
)
from ..resources.lambda_resource import LambdaResource

def make_scan_lambda_sensor(
	job: JobDefinition,
	function_name: str,
	qualifier: str,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED
) -> SensorDefinition:
    @sensor(
    	description=("Check completion status on lambda function to processcompressed images"),
		minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
    	name="lambda_function_sensor",
		job=job,
    )
    def lambda_function_sensor(
    	context: SensorEvaluationContext,
    	lambda_client: LambdaResource,
    ):
        context.log.info(f"Poking lambda function: {function_name} with qualifier: {qualifier} for updates..")
        configuration = lambda_client.get_function(function_name, qualifier)["Configuration"]
        state = configuration["State"]
        status = configuration["LastUpdateStatus"]
        if state in ["Failed", "Inactive"] or status == "Failed":
            yield SkipReason("Lambda function state sensor failed because the Lambda is in a failed state")
        elif state == "Pending" or \
    		(state == "Active" & status == "InProgress"):
            yield SkipReason(f"Function {function_name} is still running.")
        elif state == "Active" & status == "Successful":
            yield RunRequest(
                asset_selection=["stage/compressed_images"],
    			run_key=f'process_compressed_images:{status}',
                run_config={}
    		)
    return lambda_function_sensor