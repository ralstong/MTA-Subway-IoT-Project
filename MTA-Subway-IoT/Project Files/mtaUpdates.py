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

import vehicle,alert,tripupdate
class mtaUpdates(object):

    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')
    
    # feed url depends on the routes to which you want updates
    # here we are using feed 1 , which has lines 1,2,3,4,5,6,S
    # While initializing we can read the API Key and add it to the url
    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='
    
    VCS = {1:"INCOMING_AT", 2:"STOPPED_AT", 3:"IN_TRANSIT_TO"}    
    tripUpdates = []
    vehicleUpdates = []
    alerts = []
    timest = []	
    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey

    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib2.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            print "Error while connecting to mta server " +str(e)
	

	timestamp = feed.header.timestamp
        nytime = datetime.fromtimestamp(timestamp,self.TIMEZONE)
	self.timest = nytime
	for entity in feed.entity:
	    # Trip update represents a change in timetable
	    if entity.trip_update and entity.trip_update.trip.trip_id:
		update = tripupdate.tripupdate()
		
		##### INSERT TRIPUPDATE CODE HERE ####			
		update.tripId = entity.trip_update.trip.trip_id
		update.routeId = entity.trip_update.trip.route_id
		update.startDate = entity.trip_update.trip.start_date
		#update.direction = entity.trip_update.trip.trip_id[10]
		#update.direction = entity.trip_update.stop_time_update.stop_id[3]
		update.starttime = entity.trip_update.trip.start_time
		i = 1
		for stopid in entity.trip_update.stop_time_update:
		    update.futureStops[stopid.stop_id] = [stopid.arrival.time,stopid.departure.time]
		    if i==1:
			update.direction=stopid.stop_id[3]
		    i = i+1
#		    print update.futureStops[stopid.stop_id]
		self.tripUpdates.append(update)
	    if entity.vehicle and entity.vehicle.trip.trip_id:
	    	v = vehicle.vehicle()
		##### INSERT VEHICLE CODE HERE #####
		v.currentStopNumber = entity.vehicle.current_stop_sequence
		v.currentStopId = entity.vehicle.stop_id
		v.timestamp = entity.vehicle.timestamp
		v.currentStopStatus = self.VCS[entity.vehicle.current_status+1]
	        v.tripId = entity.vehicle.trip.trip_id
		self.vehicleUpdates.append(v)
	    if entity.alert and entity.alert.informed_entity:
                a = alert.alert()
		#### INSERT ALERT CODE HERE #####
	        for alerttrip in entity.alert.informed_entity:
		    alert.tripId = alerttrip.trip.trip_id
		    alert.routeId = alerttrip.trip.route_id
	   #         alert.startDate = alertrrip.trip.start_date
                    a.tripId = alerttrip.trip.trip_id
                    a.routeId = alerttrip.trip.route_id
           #     a.startDate = entity.trip_update.trip.start_date
			
        for i in self.tripUpdates:
	    for j in self.vehicleUpdates:
		if i.tripId == j.tripId:
	            i.vehicleData=[j.currentStopId,j.currentStopStatus,j.timestamp]
#		    print i.tripId,i.routeId,j.tripId,j.currentStopNumber     		

	return self.tripUpdates,self.timest


    # END OF getTripUpdates method
