import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt
import decimal


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

	def permute(x):
		shuffled = [xi for xi in x]
		shuffle(shuffled)
		return shuffled

	def avg(x): # Average
		return sum(x)/len(x)

	def stddev(x): # Standard deviation.
		m = incoming_outgoing.avg(x)
		return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

	def cov(x, y): # Covariance.
		return sum([(xi-incoming_outgoing.avg(x))*(yi-incoming_outgoing.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

	def corr(x, y): # Correlation coefficient.
		if incoming_outgoing.stddev(x)*incoming_outgoing.stddev(y) != 0:
			return incoming_outgoing.cov(x, y)/(incoming_outgoing.stddev(x)*incoming_outgoing.stddev(y))

	def p(x, y):
		c0 = incoming_outgoing.corr(x, y)
		corrs = []
		for k in range(0, 2000):
			y_permuted = incoming_outgoing.permute(y)
			corrs.append(incoming_outgoing.corr(x, y_permuted))
		return len([c for c in corrs if abs(c) > c0])/len(corrs)



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

		inter = []
		for k in range(len(in_l)):
			for l in range(len(in_l)):
				if in_l[k]['end station'] == out_l[l]['station']:
					inter.append([in_l[k],out_l[l]])

		print(inter[:3])



		inct = []
		outt = []
		for i in inter:
			#print(i[0]['# incoming trips'])
			inct.append(i[0]['# incoming trips'])
			outt.append(i[1]['# outgoing trips'])

		print(inct)

		pval = incoming_outgoing.p(inct,outt)

		correl = incoming_outgoing.corr(inct,outt)

		covarr = incoming_outgoing.cov(inct,outt)

		stdevi = incoming_outgoing.stddev(inct)

		stdevo = incoming_outgoing.stddev(outt)

		avgi = incoming_outgoing.avg(inct)



		print("Incoming Avg: "+ str(avgi))
		print("Incoming Stdev: "+ str(stdevi) + " Outgoing Stdev: "+ str(stdevo))
		print("Covariance: ", covarr)
		print("Correlation: ", correl)
		print("P-Value: ",decimal.Decimal(pval))
		











		#inc_out_list = inc_out_list.append(in_l)
		#inc_out_list = inc_out_list.append(out_l)




		#print(in_l[0]['end station']== out_l[0]['station'])

		print(len(out_l))




		
		

		

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
		inter = [{'features':x} for x in inter]

		repo['jtbloom_rfballes_medinad.incoming_outgoing'].insert_many(inter)

			


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

incoming_outgoing.execute()