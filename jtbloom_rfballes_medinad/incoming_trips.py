import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class incoming_trips(dml.Algorithm):
	contributor = 'jtbloom_rfballes_medinad'
	reads = ['jtbloom_rfballes_medinad.tripHistory']
	writes = ['jtbloom_rfballes_medinad.incoming_trips']

	
	def project(tripset, item):
		return [item(t) for t in tripset]	

	def product(R, S):
		return [(t,u) for t in R for u in S]

	def aggregate(R, f):
		keys = {r[0][0] for r in R}
		return [(key, f([v for ((k,v), lat, lon) in R if k == key])) for key in keys]	

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
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

		repo.dropCollection('jtbloom_rfballes_medinad.incoming_trips')
		repo.createCollection('jtbloom_rfballes_medinad.incoming_trips')


		trip_history = repo.jtbloom_rfballes_medinad.tripHistory.find()

		trip_list = []

    	#selecting from jtbloom_rfballes_medinad.tripHistory
		for trip_dict in trip_history:
			trip_selection = {}
			for item in trip_dict:
				trip_selection['start station name'] = trip_dict['start station name']
				trip_selection['end station name'] = trip_dict['end station name']
				trip_selection['end station latitude'] = trip_dict['end station latitude']
				trip_selection['end station longitude'] = trip_dict['end station longitude']
				trip_selection['bikeid'] = trip_dict['bikeid']
			trip_list.append(trip_selection)


		incoming = incoming_trips.project(trip_list, lambda t: ((t['end station name'], t['bikeid']), t['end station latitude'], t['end station longitude']))
		#print(incoming)
		num_trips = incoming_trips.aggregate(incoming, incoming_trips.count_trips)
		#num_trips = incoming_trips.product(incoming, num_trips)
		#print(num_trips)
		num_trips = [{'# incoming trips': n, 'station': s} for (s, n) in num_trips]
		repo['jtbloom_rfballes_medinad.incoming_trips'].insert_many(num_trips)

			


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

incoming_trips.execute()