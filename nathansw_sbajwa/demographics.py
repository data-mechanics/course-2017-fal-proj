import json
import dml
import prov.model
import datetime
import uuid
import os 

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

		curr_dir = os.getcwd()
		new_dir = curr_dir + "\\nathansw_sbajwa\\" 

		with open(new_dir + 'Race.json') as race_json:
			r = json.load(race_json)
		s = json.dumps(r, indent=4)
		test_r = json.loads(s)
		repo.dropCollection("race")
		repo.createCollection("race")
		repo['nathansw_sbajwa.race'].insert_one(r)

		with open(new_dir + 'MeansOfCommuting.json') as commuting_json:
			r = json.load(commuting_json)
		s = json.dumps(r, indent=4)
		repo.dropCollection("commuting")
		repo.createCollection("commuting")
		repo['nathansw_sbajwa.commuting'].insert_one(r)

		with open(new_dir + 'PovertyRates.json') as poverty_json:
			r = json.load(poverty_json)
		s = json.dumps(r, indent=4)
		repo.dropCollection("povertyrates")
		repo.createCollection("povertyrates")
		repo['nathansw_sbajwa.povertyrates'].insert_one(r)

		with open(new_dir + 'HouseholdIncome.json') as income_json:
			r = json.load(income_json)

		## removes $ in all of the nested keys within the JSON file 
		for town in r.keys():
			# Preps variables to alter dict with
			toReplace = {}
			toDelete = []
			for old_key in r[town]:
    			# ex: '$25,000-34,999' -> '25,000-34,999'
				new_key = old_key.replace('$', '')
        		# only continue if the original key had a $ that needed to be removed
				if new_key != old_key:
					# puts new key in seperate dict
					toReplace[new_key] = r[town][old_key]
					# adds old key to list of keys to be deleted
					toDelete += [old_key]
			# merges two dicts i.e. r[town] contains both old and new keys ($ and no $)
			r[town].update(toReplace)
			# deletes old keys from r[town] leaving only kys with no $
			for key in toDelete:
				del r[town][key]

		# # original code in case mine doesn't actually work #

		# ## removes $ in all of the nested keys within the JSON file 
		# for town in r.keys():
		# 	print('Town: ' + str(town))
		# 	for old_key in r[town]:
		# 		print("old key = " + str(old_key))
  #   			# ex: '$25,000-34,999' -> '25,000-34,999'
		# 		new_key = old_key.replace('$', '')
  #       		# only continue if the original key had a $ that needed to be removed
		# 		if new_key != old_key:
		# 			print("new key = " + str(new_key))
		# 			print("r[town][old_key] = " + str(r[town][old_key]))

		# 			r[town][new_key] = r[town][old_key]

		# 			print("r[town][new_key] = " + str(r[town][new_key]))
  #          			# remove the old nested key from the JSON object
		# 			del r[town][old_key]
		# 			print("KEY REPLACED")

		s = json.dumps(r, indent=4)
		repo.dropCollection("householdincome")
		repo.createCollection("householdincome")
		repo['nathansw_sbajwa.householdincome'].insert_one(r)

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

		this_script = doc.agent('alg:nathansw_sbajwa#demographics', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
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