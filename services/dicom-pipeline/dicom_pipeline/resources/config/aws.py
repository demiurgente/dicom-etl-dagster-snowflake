from collections import namedtuple
import os

AWS_PROFILE_NAME = os.getenv("AWS_PROFILE_NAME", "")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "")

# provider bucket source configuration
AWS_PROVIDER_ACCESS_KEY_ID = os.getenv("AWS_PROVIDER_ACCESS_KEY_ID", "")
AWS_PROVIDER_SECRET_ACCESS_KEY = os.getenv("AWS_PROVIDER_SECRET_ACCESS_KEY", "")
s3_conf_provider_dict = {
    "use_unsigned_session": True,
    "region_name": AWS_REGION_NAME,
    "profile_name": AWS_REGION_NAME,
    "aws_access_key_id": AWS_PROVIDER_ACCESS_KEY_ID,
    "aws_secret_access_key": AWS_PROVIDER_SECRET_ACCESS_KEY
}

# instantiate configs for s3 client buckets
s3_conf_prod_dict = s3_conf_stage_dict = s3_conf_raw_dict = s3_conf_dict = s3_conf_provider_dict

# provider bucket access s3 client to copy/sync objects
AWS_PROVIDER_BUCKET = os.getenv("AWS_PROVIDER_BUCKET", "")
AWS_PROVIDER_PREFIX = os.getenv("AWS_PROVIDER_PREFIX", "")
s3_conf_provider_dict['s3_bucket'] = AWS_PROVIDER_BUCKET
s3_conf_provider_dict['s3_prefix'] = AWS_PROVIDER_PREFIX
S3Config = namedtuple('S3Config', s3_conf_provider_dict)
s3_conf_provider = S3Config(**s3_conf_provider_dict)

# owned bucket source configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_BUCKET = os.getenv("AWS_BUCKET", "")
AWS_PREFIX = os.getenv("AWS_PREFIX", "")
s3_conf_dict['s3_bucket'] = AWS_BUCKET
s3_conf_dict['s3_prefix'] = AWS_PREFIX
s3_conf_dict['aws_access_key_id'] = AWS_ACCESS_KEY_ID
s3_conf_dict['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY
s3_conf = S3Config(**s3_conf_dict)

# raw layer bucket to put raw source files to
AWS_RAW_BUCKET = os.getenv("AWS_RAW_BUCKET", "")
AWS_RAW_PREFIX = os.getenv("AWS_RAW_PREFIX", "")
s3_conf_raw_dict['s3_bucket'] = AWS_RAW_BUCKET
s3_conf_raw_dict['s3_prefix'] = AWS_RAW_PREFIX
s3_conf_raw = S3Config(**s3_conf_raw_dict)

# stage layer bucket to put normalized files to
AWS_STAGE_BUCKET = os.getenv("AWS_STAGE_BUCKET", "")
AWS_STAGE_PREFIX = os.getenv("AWS_STAGE_PREFIX", "")
s3_conf_stage_dict['s3_bucket'] = AWS_STAGE_BUCKET
s3_conf_stage_dict['s3_prefix'] = AWS_STAGE_PREFIX
s3_conf_stage = S3Config(**s3_conf_stage_dict)

# prod layer bucket to put processed files to
AWS_PROD_BUCKET = os.getenv("AWS_PROD_BUCKET", "")
AWS_PROD_PREFIX = os.getenv("AWS_PROD_PREFIX", "")
s3_conf_prod_dict['s3_bucket'] = AWS_PROD_BUCKET
s3_conf_prod_dict['s3_prefix'] = AWS_PROD_PREFIX
s3_conf_prod = S3Config(**s3_conf_prod_dict)

# service to compress images from services/img-compressor
LAMBDA_FUNCTION_NAME = os.getenv("AWS_LAMBDA_FUNCTION_NAME", "")
LAMBDA_QUALIFIER = os.getenv("AWS_LAMBDA_QUALIFIER", "") 
# limit for listing keys to avoid deadlocks on sparse dirs
S3_MAX_LIST_KEYS = 1000
