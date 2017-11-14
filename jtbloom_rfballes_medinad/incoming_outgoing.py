import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class incoming_outgoing(dml.Algorithm):
	contributor = 'jtbloom_rfballes_medinad'
	reads = ['jtbloom_rfballes_medinad.incoming_trips', 'jtbloom_rfballes_medinad.outgoing_trips']
	writes = ['jtbloom_rfballes_medinad.incoming_outgoing']

	
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

	def intersect(R, S):
		return [t for t in R if t in S]


	def avg(x): # Average
		return sum(x)/len(x)

	def stddev(x): # Standard deviation.
		m = avg(x)
		return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

	def cov(x, y): # Covariance.
		return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

	def corr(x, y): # Correlation coefficient.
		if stddev(x)*stddev(y) != 0:
			return cov(x, y)/(stddev(x)*stddev(y))



	@staticmethod
	def execute(trial = False):

		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()


    	# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

		repo.dropCollection('jtbloom_rfballes_medinad.incoming_outgoing')
		repo.createCollection('jtbloom_rfballes_medinad.incoming_outgoing')


		

		inc = repo.jtbloom_rfballes_medinad.incoming_trips.find()
		out = repo.jtbloom_rfballes_medinad.outgoing_trips.find()


		inc_out_list = []

		in_l = []
		out_l = []

		for i in inc:
			in_l.append(i)
		for j in out: 
			out_l.append(j)

		#inc_out_list = inc_out_list.append(in_l)
		#inc_out_list = inc_out_list.append(out_l)




		print(in_l[1])

		print(out_l[1])




		
		

		

		#trip_list = []

    	#selecting from jtbloom_rfballes_medinad.tripHistory
		#for trip_dict in trip_history:
		#	trip_selection = {}
		#	for item in trip_dict:
		#		trip_selection['start station name'] = trip_dict['start station name']
		#		trip_selection['end station name'] = trip_dict['end station name']
		#		trip_selection['end station latitude'] = trip_dict['end station latitude']
		#		trip_selection['end station longitude'] = trip_dict['end station longitude']
		#		trip_selection['bikeid'] = trip_dict['bikeid']
		#	trip_list.append(trip_selection)



		#incoming = incoming_trips.project(trip_list, lambda t: ((t['end station name'], t['bikeid']), t['end station latitude'], t['end station longitude']))
		#lat_lon = incoming_trips.project(trip_list, lambda t:(t['end station name'], t['end station latitude'], t['end station longitude']))
		#print(incoming)
		#num_trips = incoming_trips.aggregate(incoming, incoming_trips.count_trips)
		#num_trips = incoming_trips.product(incoming, num_trips)
		#print(num_trips)

		

		#num_trips = [{'# incoming trips': n, 'end station': s} for (s, n) in num_trips]
		

		#for i in range(len(num_trips)):
		#	print(num_trips[i]['end station'])


		#repo['jtbloom_rfballes_medinad.incoming_trips'].insert_many(num_trips)

			


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

incoming_outgoing.execute()