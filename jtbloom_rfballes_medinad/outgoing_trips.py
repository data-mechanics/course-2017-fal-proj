import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class outgoing_trips(dml.Algorithm):
	contributor = 'jtbloom_rfballes'
	reads = ['jtbloom_rfballes.tripHistory']
	writes = ['jtbloom_rfballes.outgoing_trips']

	#taken from notes 
	def project(tripset, item):  
		return {item(t) for t in tripset}	

	#taken from notes 	
	def aggregate(R, f):
		keys = {r[0] for r in R}
		return {(key, f([v for (k,v) in R if k == key])) for key in keys}	

	#buil for the aggregate to be able to count trips instead of adding bike id's together
	def count_trips(iterable):
		num = 0
		for i in iterable:
			num += 1
		return num 



	@staticmethod
	def execute(trial = False):

		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()


    	# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes', 'jtbloom_rfballes')

		repo.dropCollection('jtbloom_rfballes.outgoing_trips')
		repo.createCollection('jtbloom_rfballes.outgoing_trips')


		trip_history = repo.jtbloom_rfballes.tripHistory.find()

		trip_list = []

    	#selecting from jtbloom_rfballes.tripHistory
		for trip_dict in trip_history:
			trip_selection = {}
			for item in trip_dict:
				trip_selection['start station name'] = trip_dict['start station name']
				trip_selection['end station name'] = trip_dict['end station name']
				trip_selection['bikeid'] = trip_dict['bikeid']
			trip_list.append(trip_selection)

		#projecting to (trip end station, bike id) for aggregation 
		outgoing = outgoing_trips.project(trip_list, lambda t: (t['start station name'], t['bikeid'])) 

		num_trips = outgoing_trips.aggregate(outgoing, outgoing_trips.count_trips) #aggregation 
		num_trips = [{'# outgoing trips': n, 'station': s} for (s, n) in num_trips] #turns tuple list back to dictionary
		repo['jtbloom_rfballes.outgoing_trips'].insert_many(num_trips)

			


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

#outgoing_trips.execute()