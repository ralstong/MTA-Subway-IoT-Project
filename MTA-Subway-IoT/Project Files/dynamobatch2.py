# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
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

import boto.s3.connection
#from boto.s3.connection import S3Connection
#s3_conn=S3Connection(
from boto.s3.key import Key

### YOUR CODE HERE ####
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

#setting S3
#s3_conn=boto.connect_s3(aws_access_key_id = assumedRoleObject.credentials.access_key,aws_secret_access_key = assumedRoleObject.credentials.secret_key,security_token=assumedRoleObject.credentials.session_token)
#bucket = conn.create_bucket('mtaedisondatathu3')
#k=Key(bucket)

DYNAMODB_TABLE_NAME = 'mtaData'
# Prepare DynamoDB client
client_dynamo = boto.dynamodb2.connect_to_region('us-east-1',aws_access_key_id=assumedRoleObject.credentials.access_key,aws_secret_access_key=assumedRoleObject.credentials.secret_key,security_token=assumedRoleObject.credentials.session_token)

from boto.dynamodb2.table import Table
table_dynamo = Table(DYNAMODB_TABLE_NAME, connection=client_dynamo)

try:
  mtaData = Table.create('mtaData', schema=[HashKey('tripId')],connection = client_dynamo)
  mtaData = Table('mtaData', schema = [HashKey('tripId')], connection = client_dynamo)
  time.sleep(.12)
except:
  mtaData = Table('mtaData', schema = [HashKey('tripId')], connection = client_dynamo)

#########before this is all AWS requirements
importmta = mtaUpdates.mtaUpdates('80e73d31e83802678e719e770763da90')

#tu,timest =  importmta.getTripUpdates()
#print t[10].futureStops,v[0].currentStopNumber,timest

def up_thread():
	listcsv=list()
	l=[]
	try:
		while(True):
		        tu,timest =  importmta.getTripUpdates()
        	        print 'Entering feed\n'
	      #  with mtaData.batch_write() as batch:
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
				l = [str(t.tripId),str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),str(day),str(route),str(int(str(time96_dt)[14:16])+int(str(time96_dt)[11:13])*60) if time96_dt!='None' else 'None',str(int(str(time42_dt)[14:16])+int(str(time42_dt)[11:13])*60) if time42_dt!='None' else 'None']
				listcsv.append(l)
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
				#l = [str(t.tripId),str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),str(day),str(route),str(time96),str(time42)]
				#l = [str(t.tripId),str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),str(day),str(route),time96_dt!='None'?str(int(str(time96_dt)[14:16])+int(str(time96_dt)[11:13])*60):'None',time42_dt!='None'?str(int(str(time42_dt)[14:16])+int(str(time42_dt)[11:13])*60):'None']
				l = [str(t.tripId),str(int(str(timest)[14:16])+int(str(timest)[11:13])*60),str(day),str(route),str(int(str(time96_dt)[14:16])+int(str(time96_dt)[11:13])*60) if time96_dt!='None' else 'None',str(int(str(time42_dt)[14:16])+int(str(time42_dt)[11:13])*60) if time42_dt!='None' else 'None']
				listcsv.append(l)
		   	    else:
			    	pass
			print 'Done entering\n'
			time.sleep(2)
		#	listcsv.append(l)
#	except KeyboardInterrupt:
			#print listcsv
			out = open('out1.csv', 'a')
                	for row in listcsv:
                		for column in row:
                			out.write('%s,' % column)
                		out.write('\n')
                	out.close()
#                        k.key='mtafile'
#			k.set_contents_from_filename('out1.csv')

#	        time.sleep(2)
	except KeyboardInterrupt:
				exit
def diffdates(d1, d2):
    return (time.mktime(time.strptime(d2,"%Y-%m-%d %H:%M:%S"))- time.mktime(time.strptime(d1, "%Y-%m-%d %H:%M:%S")))

def del_thread():
        while(1):
		tu,timest = importmta.getTripUpdates()
                print 'Deleting items\n'
	        retr = mtaData.scan()
		with mtaData.batch_write() as batch:
		    for t in retr:
	                d1t =t['timeStamp']
	                d1 = d1t[:-6]
	                d2s =str(timest)
	                d2 = d2s[:-6]
	                diff = diffdates(d1, d2)
	                if (diff > 120):
	                    batch.delete_item(tripId=t['tripId'])
 	        print 'Any outdated items have been deleted\n'
	        time.sleep(60)

if __name__ == "__main__":
#    main()
    upd = threading.Thread(name='upd', target=up_thread)
#    delt = threading.Thread(name='delt', target=del_thread)
#    delt.setDaemon(True)
    upd.setDaemon(True) 
#    delt.start()
    time.sleep(1)
    upd.start()
    try:
	while(True):
	   pass
    except KeyboardInterrupt:
	print 'Interrupted!\n'
#        delt.join(0)
        upd.join(0)
        sys.exit(1) 



