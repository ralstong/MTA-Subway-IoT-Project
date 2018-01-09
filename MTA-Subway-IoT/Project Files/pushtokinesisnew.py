# This program sends the data to kinesis. You do not need to modify this code except the Kinesis stream name.
# Usage python pushToKinesis.py <file name>
# a lambda function will be triggered as a result, that will send it to AWS ML for classification
# Usage python pushToKinesis.py <csv file name with extension>

import sys,csv,json
import boto3
import time
import boto
from botocore.exceptions import ClientError
import aws
import json,time,sys
from collections import OrderedDict
from threading import Thread
import sys
import boto
import boto.dynamodb2
import time
import datetime
import dateutil.parser
import threading
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import BatchTable
import json as simplejason
import boto3
from boto3.dynamodb.conditions import Key,Attr
import boto
import boto.sns
import json
import logging

import tripupdate,vehicle,alert,mtaUpdates,aws

import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict

from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf

import vehicle,alert,tripupdate

import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict

from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf


sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws

TIMEZONE = timezone('America/New_York')
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

#s3_conn = boto3.client('kinesis',aws_access_key_id =assumedRoleObject.credentials.access_key,aws_secret_access_key = assumedRoleObject.credentials.secret_key,aws_session_token=assumedRoleObject.credentials.session_token)

client_dynamo = boto.dynamodb2.connect_to_region('us-east-1',aws_access_key_id=assumedRoleObject.credentials.access_key,aws_secret_access_key=assumedRoleObject.credentials.secret_key,security_token=assumedRoleObject.credentials.session_token)


KINESIS_STREAM_NAME = 'mtaStream'
importmta = mtaUpdates.mtaUpdates('') #key value


def main(fileName):
    # connect to kinesis
    kinesis = aws.getClient('kinesis','us-east-1')
    data = [] # list of dictionaries will be sent to kinesis
    listcsv=list()
    l=[]
    try:
    		while(True):
		        tu,timest =  importmta.getTripUpdates()
        	        print 'Entering feed\n'
			listlcl=[]
			listexp=[]
		        for t in tu:
				if t.routeId=='1' and t.direction=='S':
					if t.vehicleData[0]=='120S' and t.vehicleData[1]=='STOPPED_AT':
						time96=t.vehicleData[2]
						time96_dt = datetime.fromtimestamp(time96,TIMEZONE)
					elif '120S' in t.futureStops.keys():
						time96=t.futureStops['120S'][0]
						time96_dt = datetime.fromtimestamp(time96,TIMEZONE)
					else:
						time96='None'
						time96_dt='None'
					if t.vehicleData[0]=='127S' and t.vehicleData[1]=='STOPPED_AT':
        	                        	time42=t.vehicleData[2]
						time42_dt = datetime.fromtimestamp(time42,TIMEZONE)	
        	                	elif '127S' in t.futureStops.keys():
						time42=t.futureStops['127S'][0]
						time42_dt = datetime.fromtimestamp(time42,TIMEZONE)
					else:
						time42='None'
						time42_dt='None'
		    			if timest.strftime("%a")=='Sun' or timest.strftime("%a")=='Sat':
                    	   		 	day='Weekend'
                    			else:
                        			day='Weekday'
    		    			route='local'
		       			#mtaData.put_item(data={'tripId':t.tripId,'timeStamp':str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),'Day of the week':day,'route':route,'Time at 96th':time96,'Time at 42nd':time42},overwrite=True)
					if time96_dt!='None' and time42_dt!='None':
						l = [str(t.tripId),str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),str(day),str(route),str(int(str(time96_dt)[14:16])+int(str(time96_dt)[11:13])*60) if time96_dt!='None' else 'None',str(int(str(time42_dt)[14:16])+int(str(time42_dt)[11:13])*60) if time42_dt!='None' else 'None']
						listlcl.append(l)
	            		elif (t.routeId=='2' or t.routeId=='3') and t.direction=='S':
			    		if t.vehicleData[0]=='120S' and t.vehicleData[1]=='STOPPED_AT':
                                		time96=t.vehicleData[2]
						time96_dt = datetime.fromtimestamp(time96,TIMEZONE)
                        		elif '120S' in t.futureStops.keys():
                                		time96=t.futureStops['120S'][0]
						time96_dt = datetime.fromtimestamp(time96,TIMEZONE)
					else:
						time96='None'
						time96_dt='None'
                        		if t.vehicleData[0]=='127S' and t.vehicleData[1]=='STOPPED_AT':
                                		time42=t.vehicleData[2]
						time42_dt = datetime.fromtimestamp(time42,TIMEZONE)
                        		elif '127S' in t.futureStops.keys():
                                		time42=t.futureStops['127S'][0]
						time42_dt = datetime.fromtimestamp(time42,TIMEZONE)
					else:
						time42='None'
						time42_dt='None'
                        		if timest.strftime("%a")=='Sun' or timest.strftime("%a")=='Sat':
                                		day='Weekend'
                        		else:
                                		day='Weekday'
					route='exp'
		        		#mtaData.put_item(data={'tripId':t.tripId,'timeStamp':str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),'Day of the week':day,'route':route,'Time at 96th':time96,'Time at 42nd':time42},overwrite=True)
					if time96_dt!='None' and time42_dt!='None':
						l = [str(t.tripId),str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),str(day),str(route),str(int(str(time96_dt)[14:16])+int(str(time96_dt)[11:13])*60) if time96_dt!='None' else 'None',str(int(str(time42_dt)[14:16])+int(str(time42_dt)[11:13])*60) if time42_dt!='None' else 'None']
						listexp.append(l)
		   		else:
			    		pass
				
				for i in listlcl:
			        	for j in listexp:
						if i[4]!='None' and j[4]!='None':
							if int(i[4])<=int(j[4]):
								row = {'timest':i[1], 'day':i[2], 'time96lcl':i[4], 'time96exp':j[4]}
								kinesis.put_record(StreamName=KINESIS_STREAM_NAME, Data=json.dumps(row), PartitionKey='0')
			print 'Done Entering\n'    
    except KeyboardInterrupt:
		exit


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Missing arguments"
        sys.exit(-1)
    if len(sys.argv) > 2:
        print "Extra arguments"
        sys.exit(-1)
    try:
        fileName = sys.argv[1]
        main(fileName)
    except Exception as e:
        raise e
