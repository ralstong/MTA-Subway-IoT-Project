import time,sys,json
import boto3
import boto
import aws
from botocore.exceptions import ClientError


ACCOUNT_ID = ''
IDENTITY_POOL_ID = 'us-east-1:'
ROLE_ARN = ''

# Use cognito to get an identity.
cognito = boto.connect_cognito_identity()
cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
oidc = cognito.get_open_id_token(cognito_id['IdentityId'])

# Further setup your STS using the code below
sts = boto.connect_sts()
assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])

sys.path.append('../utils')

ml_conn = boto3.client('machinelearning','us-east-1',aws_access_key_id =assumedRoleObject.credentials.access_key,aws_secret_access_key = assumedRoleObject.credentials.secret_key,aws_session_token=assumedRoleObject.credentials.session_token)

TIMESTAMP  =  time.strftime('%Y-%m-%d-%H-%M-%S')
S3_BUCKET_NAME = "mtaedisondatathu3"
S3_FILE_NAME = 'outs3-consolidated-1.csv'
S3_URI = "s3://{0}/{1}".format(S3_BUCKET_NAME, S3_FILE_NAME)
DATA_SCHEMA = "aml.csv.schema"

response_ds = ml_conn.create_data_source_from_s3(
    DataSourceId='ds_id' + TIMESTAMP,
    DataSourceName='ml_data1',
    DataSpec={
        'DataLocationS3': S3_URI,
        'DataRearrangement': '{\"splitting\":{\"percentBegin\":10,\"percentEnd\":60}}',
        'DataSchemaLocationS3': "s3://{0}/{1}".format(S3_BUCKET_NAME, 'aml.csv.schema')
    },
    ComputeStatistics=True
)

response_createml = ml_conn.create_ml_model(
    MLModelId='ml_id'+TIMESTAMP,
    MLModelName='ml_model1',
    MLModelType='BINARY',
    TrainingDataSourceId = 'ds_id' + TIMESTAMP,
)

response_eval = ml_conn.create_evaluation(
    EvaluationId='eval_id'+TIMESTAMP,
    EvaluationName='ml_eval1',
    MLModelId='ml_id'+TIMESTAMP,
    EvaluationDataSourceId='ds_id' + TIMESTAMP,
)

response_end = ml_conn.create_realtime_endpoint(
    MLModelId='ml_id'+TIMESTAMP,
)

