import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class neighborhood_station_income_out(dml.Algorithm):
	contributor = 'jtbloom_rfballes_medinad'
	reads = ['jtbloom_rfballes_medinad.stationsByneighborhood', 'jtbloom_rfballes_medinad.neighborhood_income_final']
	writes = ['jtbloom_rfballes_medinad.neighborhood_station_income_out']

	def intersect(R, S):
		return [t for t in R if t in S]

	def project(R, p):
		return [p(t) for t in R]

	def aggregate(R, f):
		keys = {r[0] for r in R}
		return [(key, f([v1 for (k,v, v1) in R if k == key])) for key in keys]


	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

		repo.dropCollection('jtbloom_rfballes_medinad.neighborhood_station_income_out')
		repo.createCollection('jtbloom_rfballes_medinad.neighborhood_station_income_out')

		s_by_n = list(repo['jtbloom_rfballes_medinad.stationsByneighborhood'].find())
		income = list(repo['jtbloom_rfballes_medinad.neighborhood_income_final'].find())
		#print(s_by_n)


		tripsby_neighborhood = neighborhood_station_income_out.aggregate(neighborhood_station_income_out.project(s_by_n, lambda t: (t['Neighborhood'], t['station'], t['trips'])), sum)
		print(tripsby_neighborhood)
		print(len(tripsby_neighborhood))




		out_proj = neighborhood_station_income_out.project(out, lambda t: (t['station'], t['# outgoing trips']))
		#print(len(out_proj))

		#test = []
		# pair = [j[1] for j in s_proj]
		# pair2 = [i[0] for i in out_proj]

		# count = 0
		# for s in pair:
		# 	for k in pair2:
		# 		if s == k:
		# 			count+=1

		# print(pair)
		# print(pair2)
		# print(count)

		#for j in s_proj:
		#	test.append(j[1])

		#for i in out_proj:
		#	test.append(i[0])
		#print(s_proj)
		#for i in range(len())


		newincout = []
		for x in s_by_n:
			for y in out:
				if x['station']!=y['station']:
					#print(x['Station'], y['station'])
					newincout.append({'Neighborhood':x['Neighborhood'], 'Station':x['Station'],"Outgoing Trips":y['# outgoing trips']})

		#print(len(newincout))


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

neighborhood_station_income_out.execute()
