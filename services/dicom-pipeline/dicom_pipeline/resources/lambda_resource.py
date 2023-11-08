from typing import Any, Optional
import boto3
import dagster._check as check
from dagster_aws.utils import construct_boto_client_retry_config
from dagster import Config, IAttachDifferentObjectToOpContext, resource
from dagster._core.definitions.resource_definition import dagster_maintained_resource
from dagster_aws.s3.resources import ResourceWithS3Configuration


def construct_lambda_client(
    max_attempts,
    region_name=None,
    endpoint_url=None,
    profile_name=None,
    use_ssl=True,
    verify=None,
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_session_token=None,
):
    check.int_param(max_attempts, "max_attempts")
    check.opt_str_param(region_name, "region_name")
    check.opt_str_param(endpoint_url, "endpoint_url")
    check.opt_str_param(profile_name, "profile_name")
    check.bool_param(use_ssl, "use_ssl")
    check.opt_str_param(verify, "verify")
    check.opt_str_param(profile_name, "aws_access_key_id")
    check.opt_str_param(profile_name, "aws_secret_access_key")
    check.opt_str_param(profile_name, "aws_session_token")

    client_session = boto3.session.Session(profile_name=profile_name)
    lambda_client = client_session.resource(
        "lambda",
        region_name=region_name,
        use_ssl=use_ssl,
        verify=verify,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        config=construct_boto_client_retry_config(max_attempts),
    ).meta.client
    return lambda_client

class LambdaConfig(Config):
    function_name: str
    invocation_type: Optional[str]
    log_type: Optional[str]
    client_context: Optional[str]
    payload: Optional[str]
    qualifier: str

class LambdaResource(ResourceWithS3Configuration, IAttachDifferentObjectToOpContext):
    """Resource that gives access to AWS Lambda function.
    
    The underlying S3 session is created by calling
    :py:func:`boto3.session.Session(profile_name) <boto3:boto3.session>`.
    The returned resource object is a lambda client, an instance of `botocore.client.lambda`.
    """
    def get_client(self) -> Any:
        return construct_lambda_client(
            max_attempts=self.max_attempts,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            profile_name=self.profile_name,
            use_ssl=self.use_ssl,
            verify=self.verify,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
        )

    def get_object_to_set_on_execution_context(self) -> Any:
        return self.get_client()

@resource(config_schema=LambdaResource.to_config_schema())
def lambda_resource(context) -> Any:
    return LambdaResource.from_resource_context(context).get_client()
