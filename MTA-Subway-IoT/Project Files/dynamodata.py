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
import json as simplejason
import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws

### YOUR CODE HERE ####

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

DYNAMODB_TABLE_NAME = 'mtaData'
# Prepare DynamoDB client
client_dynamo = boto.dynamodb2.connect_to_region('us-east-1',aws_access_key_id=assumedRoleObject.credentials.access_key,aws_secret_access_key=assumedRoleObject.credentials.secret_key,security_token=assumedRoleObject.credentials.session_token)

from boto.dynamodb2.table import Table
table_dynamo = Table(DYNAMODB_TABLE_NAME, connection=client_dynamo)

try:
  mtaData = Table.create('mtaData', schema=[HashKey('tripId'),RangeKey('timeStamp')],connection = client_dynamo)
  mtaData = Table('mtaData', schema = [HashKey('tripId'),RangeKey('timeStamp')], connection = client_dynamo)
  time.sleep(.12)
except:
  mtaData = Table('mtaData', schema = [HashKey('tripId'),RangeKey('timeStamp')], connection = client_dynamo)



#########before this is all AWS requirements
importmta = mtaUpdates.mtaUpdates('80e73d31e83802678e719e770763da90')

#tu,timest =  importmta.getTripUpdates()
#print t[10].futureStops,v[0].currentStopNumber,timest

def up_thread(cond):
	while(1):
            with cond:
	        tu,timest =  importmta.getTripUpdates()
                print 'Entering feed\n'
	        for t in tu:
	            mtaData.put_item(data={'tripId':t.tripId,'timeStamp':str(timest),'routeId':t.routeId,'startDate':t.startDate,'direction':t.direction,'currentStopId':t.vehicleData[0],'currentStopStatus':t.vehicleData[1],'vehicleTimeStamp':t.vehicleData[2],'futureStopData':json.dumps(t.futureStops)})
		print 'Done entering\n'
	        cond.notifyAll()
	    time.sleep(30)

def diffdates(d1, d2):
    return (time.mktime(time.strptime(d2,"%Y-%m-%d %H:%M:%S"))- time.mktime(time.strptime(d1, "%Y-%m-%d %H:%M:%S")))

def del_thread(cond):
        while(1):
	    with cond:
		cond.wait()
		tu,timest = importmta.getTripUpdates()
                print 'Deleting items\n'
	        retr = mtaData.scan()
		for t in retr:
	            d1t =t['timeStamp']
	            d1 = d1t[:-6]
	            d2s =str(timest)
	            d2 = d2s[:-6]
	            diff = diffdates(d1, d2)
	            if (diff > 120):
	                mtaData.delete_item(tripId=t['tripId'],timeStamp=t['timeStamp'])
 	        print 'Any outdated items have been deleted\n'
	    time.sleep(60)

if __name__ == "__main__":
#    main()
    condition = threading.Condition()
    upd = threading.Thread(name='upd', target=up_thread, args=(condition,))
    delt = threading.Thread(name='delt', target=del_thread, args=(condition,))
    delt.setDaemon(True)
    upd.setDaemon(True) 
    delt.start()
    time.sleep(1)
    upd.start()
    try:
	while(True):
	   pass
    except KeyboardInterrupt:
	print 'Interrupted!\n'
        delt.join(0)
        upd.join(0)
        sys.exit(1) 



