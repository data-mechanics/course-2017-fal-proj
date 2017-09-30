import json
import dml
import prov.model
import datetime
import uuid


class demographics(dml.Algorithm):

	

	contributor = 'nathansw_sbajwa'
	reads = []
	writes = ['nathansw_sbajwa.race', 'nathansw_sbajwa.householdincome', 'nathansw_sbajwa.povertyrates', 'nathansw_sbajwa.commuting']

	@staticmethod
	def execute(trial = False):


		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa','nathansw_sbajwa')

		with open('Race.json') as race_json:
			r = json.load(race_json)
		s = json.dumps(r, indent=4)
		repo.dropCollection("race")
		repo.createCollection("race")
		repo['nathansw_sbajwa.race'].insert_many(r)

		with open('MeansOfCommuting.json') as commuting_json:
			r = json.load(commuting_json)
		s = json.dumps(r, indent=4)
		repo.dropCollection("commuting")
		repo.createCollection("commuting")
		repo['nathansw_sbajwa.commuting'].insert_many(r)

		with open('PovertyRates.json') as poverty_json:
			r = json.load(poverty_json)
		s = json.dumps(r, indent=4)
		repo.dropCollection("povertyrates")
		repo.createCollection("povertyrates")
		repo['nathansw_sbajwa.povertyrates'].insert_many(r)

		with open('HouseholdIncome.json') as income_json:
			r = json.load(income_json)
		s = json.dumps(r, indent=4)
		repo.dropCollection("householdincome")
		repo.createCollection("householdincome")
		repo['nathansw_sbajwa.householdincome'].insert_many(r)

		repo.logout()
		endTime = datetime.datetime.now()

		return {"start": startTime, "end": endTime}


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("nathansw_sbajwa","nathansw_sbajwa")
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/sbajwa_nathansw/') # The scripts in / format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/sbajwa_nathansw/') # The data sets in / format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

		doc.add_namespace('race', 'goo.gl/V3hgSW') 
		doc.add_namespace('povertyrates', 'goo.gl/V3hgSW')
		doc.add_namespace('householdincome', 'goo.gl/V3hgSW')
		doc.add_namespace('commuting', 'goo.gl/V3hgSW')

		this.script = doc.agent('alg:nathansw_sbajwa#demographics', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('race: goo.gl/V3hgSW', {'prov:label':'Race by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource = doc.entity('povertyrates: goo.gl/V3hgSW', {'prov:label':'Poverty Rates by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource = doc.entity('householdincome: goo.gl/V3hgSW', {'prov:label':'Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource = doc.entity('commuting: goo.gl/V3hgSW', {'prov:label':'Means of Commuting by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_race = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_povertyrates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_householdincome = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_commuting = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_race, this_script)
		doc.usage(get_race, resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'?type=Race&$select=type,latitude,longitude,OPEN_DT'
              }
              )
		doc.wasAssociatedWith(get_povertyrates, this_script)
		doc.usage(get_povertyrates, resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'?type=Poverty+Rate&$select=type,latitude,longitude,OPEN_DT'
              }
              )
		doc.wasAssociatedWith(get_householdincome, this_script)
		doc.usage(get_householdincome, resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'?type=Household+Income&$select=type,latitude,longitude,OPEN_DT'
              }
              )
		doc.wasAssociatedWith(get_commuting, this_script)
		doc.usage(get_commuting, resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'?type=Commuting&$select=type,latitude,longitude,OPEN_DT'
              }
              )

		race = doc.entity('dat:nathansw_sbajwa#race', {prov.model.PROV_LABEL:'Race by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(race, this_script)
		doc.wasGeneratedBy(race, get_race, endTime)
		doc.wasDerivedFrom(race, resource, get_race, get_race, get_race)			

		povertyrates = doc.entity('dat:nathansw_sbajwa#povertyrates', {prov.model.PROV_LABEL:'Poverty by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(povertyrates, this_script)
		doc.wasGeneratedBy(povertyrates, get_povertyrates, endTime)
		doc.wasDerivedFrom(povertyrates, resource, get_povertyrates, get_povertyrates, get_povertyrates)		

		householdincome = doc.entity('dat:nathansw_sbajwa#householdincome', {prov.model.PROV_LABEL:'Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(householdincome, this_script)
		doc.wasGeneratedBy(householdincome, get_householdincome, endTime)
		doc.wasDerivedFrom(householdincome, resource, get_householdincome, get_householdincome, get_householdincome)		

		commuting = doc.entity('dat:nathansw_sbajwa#commuting', {prov.model.PROV_LABEL:'Means of Commuting by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(commuting, this_script)
		doc.wasGeneratedBy(commuting, get_commuting, endTime)
		doc.wasDerivedFrom(commuting, resource, get_commuting, get_commuting, get_commuting)		

		repo.logout()

		return doc

demographics.execute()
doc = demographics.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))