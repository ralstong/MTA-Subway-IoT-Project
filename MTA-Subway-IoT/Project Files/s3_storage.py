import time,sys,json
import boto3
import boto
import aws
from botocore.exceptions import ClientError


ACCOUNT_ID = ''
IDENTITY_POOL_ID = 'us-east-1'
ROLE_ARN = ''

# Use cognito to get an identity.
cognito = boto.connect_cognito_identity()
cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
oidc = cognito.get_open_id_token(cognito_id['IdentityId'])

# Further setup your STS using the code below
sts = boto.connect_sts()
assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])


sys.path.append('../utils')

s3_conn = boto3.client('s3',aws_access_key_id =assumedRoleObject.credentials.access_key,aws_secret_access_key = assumedRoleObject.credentials.secret_key,aws_session_token=assumedRoleObject.credentials.session_token)
#s3_conn = boto3.resource('s3')

class S3(object):
	S3 = None
	S3_BUCKET_NAME = 'mtaedisondatathu3'
	S3Bucket = None
	trainingData = None

	def __init__(self,trainingData):
		self.S3 = aws.getResource('s3','us-east-1')
		self.trainingData = trainingData

	def uploadData(self):
		if self.bucketExists():
			self.S3Bucket = self.S3.Bucket(self.S3_BUCKET_NAME)
		else:
			self.S3Bucket = self.S3.create_bucket(Bucket = self.S3_BUCKET_NAME)

		self.uploadToS3()

	def uploadToS3(self):
		with open(self.trainingData,'rb') as data:
			self.S3Bucket.Object(self.trainingData).put(Body=data)

	def bucketExists(self):
		try:
			self.S3.meta.client.head_bucket(Bucket=self.S3_BUCKET_NAME)
			return True
		except ClientError:
			return False


S3_obj = S3('aml.csv.schema')
#S3_obj('out.csv')
S3_obj.uploadData()
S3_obj.uploadToS3()





