import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt
import math
import decimal


class trips_population_correlation(dml.Algorithm):
	contributor = 'jtbloom_rfballes_medinad'
	reads = ['jtbloom_rfballes_medinad.stationsByneighborhood', 'jtbloom_rfballes_medinad.neighborhood_population']
	writes = ['jtbloom_rfballes_medinad.trips_population_correlation']

	def project(R, p):
		return [p(t) for t in R]

	def aggregate(R, f):
		keys = {r[0] for r in R}
		return [(key, f([v1 for (k,v, v1) in R if k == key])) for key in keys]

	def permute(x):
		shuffled = [xi for xi in x]
		shuffle(shuffled)
		return shuffled

	def avg(x): # Average
		return sum(x)/len(x)

	def stddev(x): # Standard deviation.
		m = trips_population_correlation.avg(x)
		return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

	def cov(x, y): # Covariance.
		return sum([(xi-trips_population_correlation.avg(x))*(yi-trips_population_correlation.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

	def corr(x, y): # Correlation coefficient.
		if trips_population_correlation.stddev(x)*trips_population_correlation.stddev(y) != 0:
			return trips_population_correlation.cov(x, y)/(trips_population_correlation.stddev(x)*trips_population_correlation.stddev(y))

	def p(x, y):
		c0 = trips_population_correlation.corr(x, y)
		corrs = []
		for k in range(0, 2000):
			y_permuted = trips_population_correlation.permute(y)
			corrs.append(trips_population_correlation.corr(x, y_permuted))
		return len([c for c in corrs if abs(c) > c0])/len(corrs)

	@staticmethod
	def execute(trial = False):

		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()


    	# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

		repo.dropCollection('jtbloom_rfballes_medinad.trips_population_correlation')
		repo.createCollection('jtbloom_rfballes_medinad.trips_population_correlation')

		trips_inc = list(repo['jtbloom_rfballes_medinad.neighborhood_station_income_out'].find())
		s_by_n = list(repo['jtbloom_rfballes_medinad.stationsByneighborhood'].find())


		neighborhood_pop = []
		for thing in repo.jtbloom_rfballes_medinad.neighborhood_population.find():
			for feature in thing['results']:
				new_dict = {'Neighborhood':feature['Neighborhood'], 'Population':feature['Population']}
				neighborhood_pop.append(new_dict)
		#print(neighborhood_pop)

		pop_proj = trips_population_correlation.project(neighborhood_pop, lambda t: [t['Neighborhood'], t['Population']])
		tripsby_neighborhood = trips_population_correlation.aggregate(trips_population_correlation.project(s_by_n, lambda t: (t['Neighborhood'], t['station'], t['trips'])), sum)

		#print(tripsby_neighborhood)


		#fix tuples for correlation
		for i in pop_proj:
			if i[0] == 'Longwood':
				i[0] = 'Longwood Medical Area'

		newincout = []
		for x in tripsby_neighborhood:
			for y in pop_proj:
				if x[0]==y[0]:
					newincout.append({'trips':x[1], 'population':y[1]})

		#print(newincout)



		population = []
		trips = []

		for i in newincout:
			i['population'] = str(i['population'])
			i['population'] = i['population'].replace(',', '')

		for item in newincout:
			pop = int(item['population'])
			population.append(pop)
			num_trips = int(item['trips'])
			trips.append(num_trips)

		



		pval = trips_population_correlation.p(population,trips)

		correl = trips_population_correlation.corr(population,trips)

		covarr = trips_population_correlation.cov(population,trips)

		stdevpop = trips_population_correlation.stddev(population)

		stdevtrips = trips_population_correlation.stddev(trips)

		avgpop = trips_population_correlation.avg(population)

		avgtrips = trips_population_correlation.avg(trips)



		print("pval = ", pval)
		print("correl = ", correl)
		print("covarr = ", covarr)
		print("stdev of population = ", stdevpop)
		print("stdev of trips = ", stdevtrips)
		print("avg population = ", avgpop)
		print("avg hubway = ", avgtrips)



	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

		 # Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('dm', 'http://datamechanics.io/data/jb_rfb_dm_proj2data/')

		this_script = doc.agent('dm:jtbloom_rfballes_medinad#trips_population_correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		stationsByneighborhood_resource = doc.entity('dm:stationsByneighborhood', {'prov:label':'Hubway Stations by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})
		neighborhood_population_resource = doc.entity('dm:neighborhood_population_resource', {'prov:label':'Population by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})
		trips_population_correlation_resource = doc.entity('dm:trips_population_correlation_resource', {'prov:label':'Correlation between trips per neighborhood and neighborhood income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})


		this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(this_run, this_script)

		doc.usage(this_run, trips_population_correlation_resource, startTime, None,
			  {prov.model.PROV_TYPE:'ont:Retrieval'})


		doc.wasAttributedTo(trips_population_correlation_resource, this_script)
		doc.wasGeneratedBy(trips_population_correlation_resource, this_run, endTime)

		doc.wasDerivedFrom(trips_population_correlation_resource, stationsByneighborhood_resource, this_run, this_run, this_run)
		doc.wasDerivedFrom(trips_population_correlation_resource, neighborhood_population_resource, this_run, this_run, this_run)


		repo.logout()

		return doc

#trips_income_correlation.execute()
