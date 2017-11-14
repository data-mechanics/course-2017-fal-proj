import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


# class jamandschools(dml.Algorithm):


#     def aggregate(R, f):
#         keys = {r[0] for r in R}
#         return [(key, f([v for (k, v) in R if k == key])) for key in keys]

#     contributor = 'alankang_xtq'
#     reads = []
#     writes = ['alankang_xtq.jamandschools']

#     @staticmethod
#   	def execute(trial = False):


# 		startTime = datetime.datetime.now()
# 		client = dml.pymongo.MongoClient()
# 		repo = client.repo
# 		repo.autenticate("alankang_xtq", "alankang_xtq")
# 		jamData = repo.alankang_xtq.jam
# 		schoolsData = repo.alankang_xtq.schools
class crashandschoolsdatatrans(dml.Algorithm):
	def aggregate(R, f):
		keys = {r[0] for r in R}
		return [(key, f([v for (k, v) in R if k == key])) for key in keys]
	contributor = 'alankang_xtq'
	reads = ['alankang_xtq.crash','alankang_xtq.schools']
	writes = ['alankang_xtq.crashandschoolsdata']

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alankang_xtq', 'alankang_xtq')
		crashData = repo.alankang_xtq.crash
		schoolsData = repo.alankang_xtq.schools

		'''cleanse the data to contain only the streets of jam
		'''
	
		crashstreets = []
		for x in crashData.find():
			if 'cross_street' in x:
				crashstreets.append(entry['cross_street'][1])

		crahsstreets = [(x,1) for x in crashstreets]



		
		

		'''computing the frequency of the jam occurs on each street
		'''

		crashstreets = aggregate(project(X, lambda t: (t[0]), sum))
			

		
		schoolsstreet= []

		'''cleanse the data to contain only streets of schools
		'''
		for x in schoolsData.find():
			if ('Name' in x) and ('Address' in x):
				schoolsstreet.append(entry['Name'],entry['Address'])


		

		'''aggregate two new created dataset jamstreets and schoolsstreet into 
		   a new one jamandschools which has the attributes Name, Street and 
		   Frequency. 
		'''

		crashandschools = []

		crashandschools = [(x[0],y[1]) for x in schoolsstreet for y in crashstreets if y[0] in x[1]]




		

		repo.dropCollection("crashandschoolsdata")
		repo.createCollection("crashandschoolsdata")
		repo['alankang_xtq.crashandschoolsdata'].insert_many(crashandschools)
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start": startTime, "end": endTime}

		@staticmethod
		def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
			client = dml.pymongo.MongoClient()
			repo = client.repo
			repo.authenticate("alankang_xtq", "alankang_xtq")
			doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
			doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
			doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
			doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
			doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
			this_script = doc.agent('alg:alankang_xtq#crashandschoolsdatatrans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
			resource_crash = doc.entity('dio:crash', {'prov:label':'Crashes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
			resource_schools = doc.entity('dio:Colleges_and_Universities', {'prov:label':'Colleges_and_Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
			get_crashandschoolsdata = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
			doc.wasAssociatedWith(get_crashandschoolsdata, this_script)
			doc.usage(get_crashandschoolsdata, resource_schools, startTime,None, {prov.model.PROV_TYPE: 'ont:Computation'})
			doc.usage(get_crashandschoolsdata, resource_crash, startTime,None, {prov.model.PROV_TYPE: 'ont:Computation'})
			CS = doc.entity('dat:alankang_xtq#crashandschoolsdata', {prov.model.PROV_LABEL:'CRASHSCHOOL', prov.model.PROV_TYPE:'ont:DataSet'})
			doc.wasAttributedTo(CS, this_script)
			doc.wasGeneratedBy(CS, get_crashandschoolsdata, endTime)
			doc.wasDerivedFrom(CS, resource_crash, get_crashandschoolsdata, get_crashandschoolsdata, get_crashandschoolsdata)
			doc.wasDerivedFrom(CS, resource_schools, get_crashandschoolsdata, get_crashandschoolsdata, get_crashandschoolsdata)

			repo.logout()
			return doc

