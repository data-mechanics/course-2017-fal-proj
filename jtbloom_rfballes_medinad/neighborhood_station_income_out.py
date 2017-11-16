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

		#data projections
		tripsby_neighborhood = neighborhood_station_income_out.aggregate(neighborhood_station_income_out.project(s_by_n, lambda t: (t['Neighborhood'], t['station'], t['trips'])), sum)
		income_proj = neighborhood_station_income_out.project(income, lambda t: [t['Neighborhood'], t['Income']])
		
		#fix tuples for correlation
		for i in income_proj:
			i[1] = i[1].replace(',', '')
			i[1] = i[1].replace('$', '')
			if i[0] == 'Longwood':
				i[0] = 'Longwood Medical Area'
			
			
		newincout = []
		for x in tripsby_neighborhood:
			for y in income_proj:
				if x[0]==y[0]:
					newincout.append({'trips':x[1], 'income':y[1]})



		repo['jtbloom_rfballes_medinad.neighborhood_station_income_out'].insert_many(newincout)

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

		this_script = doc.agent('dm:jtbloom_rfballes_medinad#neighborhood_station_income_out', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		station_resource = doc.entity('dm:stationsByneighborhood', {'prov:label':'Hubway Stations by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})
		income_resource = doc.entity('dm:neighborhood_income_final', {'prov:label':'Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})


		this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(this_run, this_script)

		doc.usage(this_run, station_resource, startTime, None,
			  {prov.model.PROV_TYPE:'ont:Retrieval'})


		doc.usage(this_run, income_resource, startTime, None,
			  {prov.model.PROV_TYPE:'ont:Retrieval'})


		doc.wasAttributedTo(neighborhood_station_income_out, this_script)
		doc.wasGeneratedBy(neighborhood_station_income_out, this_run, endTime)

		doc.wasDerivedFrom(neighborhood_station_income_out, station_resource, this_run, this_run, this_run)
		doc.wasDerivedFrom(neighborhood_station_income_out, income_resource, this_run, this_run, this_run)

		repo.logout()

		return doc


#neighborhood_station_income_out.execute()
