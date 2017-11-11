import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class CrimeRestaurants(dml.Algorithm):
	contributor = 'lc546_jofranco'
	reads = ['lc546_jofranco.crimerate', 'lc546_jofranco.permit']
	writes = ['lc546_jofranco.CrimeRestaurants']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("lc546_jofranco", "lc546_jofranco")
		crimerateData = repo.lc546_jofranco.crimerate
		#print(crimerateData)
		foodpermitData = repo.lc546_jofranco.permit
		print(foodpermitData)
		streetofcrime = crimerateData.find()
		#rint(streetofcrime)
		streetsthathadcrimes = []
		for i in streetofcrime:
			crimestreet = str.lower(i['streetname'])
			crimestreet2 = crimestreet.replace(" st", "").replace(" av", "").replace(" wy","").replace(" rd", "").replace( " pl", "").replace(" ln", "").replace(" dr", "")
			#print(crimestreet2)
			streetsthathadcrimes.append([crimestreet2])

		permits = foodpermitData.find()
		#print()
		foodpermitList = []

		'''Clean the data up by removing white spaces and removing
		   address numbers. We only want the streets.
		'''

		for i in permits:
			foodstreet = str.lower(i['address'])
			foodstreet1 = foodstreet.replace(" ","")
			foodstreet2 = ''.join([j for j in foodstreet1 if not j.isdigit()])
			foodstreet3 = [foodstreet2]
			foodpermitList.append(foodstreet3)
		#print("++++++++++++++")
		#print(foodpermitList)

		'''find the intersection of the two databases. We basically
			want to find the restaurants that are in a street that have had crimes.
			The hope is to dedut the idea that the more crimes are in the street the
			restaurant is located in, the less desirable the place is to people.
		'''
		intersection = [st for st in streetsthathadcrimes if st in foodpermitList]
		#print("+++++++++++")
		#print(intersection)

		finalList =[]
		for i in intersection:
			count = intersection.count(i)
			#sts = i.replace("[", "").replace("]","")
			sts = "".join(i)
			finalList.append({"Street": sts, "Crimes":count})

	#	print("______--------_________")
	#	print(finalList)

		''' now do an aggregation '''
		repo.dropCollection("CrimeRestaurants")
		repo.createCollection("CrimeRestaurants")
		repo['lc546_jofranco.CrimeRestaurants'].insert_many(finalList)
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start": startTime, "end": endTime}
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('lc546_jofranco', 'lc546_jofranco')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'http://developer.mbta.com/lib/')
		this_script = doc.agent('alg:lc546_jofranco#CrimeRestaurants', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:t85d-b449', {'prov:label':'All the realtime_MBTA stops in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_CrimeRestaurants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_CrimeRestaurants, this_script)
		doc.usage(get_CrimeRestaurants, resource, startTime, None,
			{prov.model.PROV_TYPE:'ont:Retrieval'}
			)
		crimerestaurants_intersection = doc.entity('dat:lc546_jofranco#crimerestaurants_intersection', {prov.model.PROV_LABEL:'realtime_MBTA stopsp in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(crimerestaurants_intersection, this_script)
		doc.wasGeneratedBy(crimerestaurants_intersection, get_CrimeRestaurants, endTime)
		doc.wasDerivedFrom(crimerestaurants_intersection, resource, get_CrimeRestaurants, get_CrimeRestaurants, get_CrimeRestaurants)
		# repo.record(doc.serialize()) # Record the provenance document.
		#repo.logout()
		return doc

		#@staticmethod
		#def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

			# client = dml.pymongo.MongoClient()
			# repo = client.repo
			# repo.authenticate("lc546_jofranco", "lc546_jofranco")

			# doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
			# doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
			# doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
			# doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
			# doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

			# this_script = doc.agent('alg:lc546_jofranco#CrimeRestaurants', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
			# resource = doc.entity('bdp:t85d-b449', {'prov:label':'School and Hospital Number in each zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
			# getRestaurantsandCtimeStreets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'StreetCrimesRestaurants', prov.model.PROV_TYPE:'ont:DataSet'})
			# doc.wasAssociatedWith(getRestaurantsandCtimeStreets, this_script)
			# doc.usage(getRestaurantsandCtimeStreets, CrimeRestaurants, startTime)
			# #CrimeRestaurants = doc.entity('dat:lc546_jofranco#StreetCrimesRestaurants', {prov.model.PROV_LABEL:'Crime And Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
			# CrimeRestaurants = doc.entity('dat:lc546_jofranco#CrimeRestaurants', {prov.model.PROV_TYPE:'ont:DataResource'})
			# doc.wasAttributedTo(CrimeRestaurants, this_script)
			# doc.wasGeneratedBy(CrimeRestaurants, getRestaurantsandCtimeStreets, endTime)
			# doc.wasDerivedFrom(CrimeRestaurants, resource, getRestaurantsandCtimeStreets, getRestaurantsandCtimeStreets, getRestaurantsandCtimeStreets)
			# #repo.record(doc.serialize()) # Record the provenance document.
			# #repo.logout()


			# return doc
CrimeRestaurants.execute()
doc = CrimeRestaurants.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
