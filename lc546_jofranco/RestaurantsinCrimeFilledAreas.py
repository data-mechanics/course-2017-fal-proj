import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class RestaurantsinCrimeFilledAreas(dml.Algorithm):
	contributor = 'lc546_jofranco'
	reads = []
	writes = ['lc546_jofranco.RestaurantsinCrimeFilledAreas']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.autenticate("lc546_jofranco", "lc546_jofranco")
		crimerateData = repo.lc546_jofranco.crime_rate
		foodpermitData = repo.lc546_jofranco.restaurant_permit
		streetofcrime = crimerateData.find()
		streetsthathadcrimes = []
		for i in streetofcrime:
			crimestreet = [str.lower(i['streetname'])]
			streetsthathadcrimes.append(street)

		permits = foodpermitData.find()
		foodpermitList = []

		'''Clean the data up by removing white spaces and removing 
		   address numbers. We only want the streets. 
		'''
		for i in permits:
			foodstreet = str.lower(i['address'])
			foodstreet1 = foodstreet.replace(" ","")
			foodstreet2 = ''.join([i for i in foodstreet1 if not i.isdigit()])
			foodstreet3 = [foodstreet2]
			foodpermitList.append(foodstreet3)

		'''find the intersection of the two databases. We basically 
			want to find the restaurants that are in a street that have had crimes. 
			The hope is to dedut the idea that the more crimes are in the street the 
			restaurant is located in, the less desirable the place is to people.
		'''
		intersection = [st for st in foodpermitList if st in streetsthathadcrimes]

		repo.dropCollection("RestaurantsinCrimeFilledAreas")
		repo.createCollection("RestaurantsinCrimeFilledAreas")
		repo.['lc546_jofranco.RestaurantsinCrimeFilledAreas'].insert_many(intersection)
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start": startTime, "end": endTime}

		@staticmethod
		def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
			client = dml.pymongo.MongoClient()
			repo = client.repo
			repo.authenticate("lc546_jofranco", "lc546_jofranco")
			doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
			doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
			doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
			doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
			doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
			this_script = doc.agent('alg:lc546_jofranco#RestaurantsinCrimeFilledAreas', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
			resource = doc.entity('bdp:xgbq-327x', {'prov:label':'StreetCrimesRestaurants', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
			getRestaurantsandCtimeStreets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'StreetCrimesRestaurants', prov.model.PROV_TYPE:'ont:DataSet'})
			doc.wasAssociatedWith(getRestaurantsandCtimeStreets, this_script)
			doc.usage(getRestaurantsandCtimeStreets, resource, startTime)
			StreetReport = doc.entity('dat:lc546_jofranco#StreetCrimesRestaurants', {prov.model.PROV_LABEL:'Crime And Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
			doc.wasAttributedTo(StreetReport, this_script)
			doc.wasGeneratedBy(StreetReport, getRestaurantsandCtimeStreets, endTime)
			doc.wasDerivedFrom(StreetReport, resource, getRestaurantsandCtimeStreets, getRestaurantsandCtimeStreets, getRestaurantsandCtimeStreets)
			return doc
RestaurantsinCrimeFilledAreas.execute()
doc = RestaurantsinCrimeFilledAreas.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




