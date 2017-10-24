import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transformationTwo(dml.Algorithm):
	contributor = 'klovett'
	reads = ['klovett.bbLocations','klovett.landBoston']
	writes = ['klovett.streetCoordinates']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('klovett', 'klovett')
		repo.dropCollection("streetCoordinates")
		repo.createCollection("streetCoordinates")

		modifiedDictionary = []
		
		for landData in repo["klovett.landBoston"].find():
			
			Address = landData['Address']
			streetName = landData['Address']
			
			streetName = streetName.replace("0","")
			streetName = streetName.replace("1","")
			streetName = streetName.replace("2","")
			streetName = streetName.replace("3","")
			streetName = streetName.replace("4","")
			streetName = streetName.replace("5","")
			streetName = streetName.replace("6","")
			streetName = streetName.replace("7","")
			streetName = streetName.replace("8","")
			streetName = streetName.replace("9","")
			if (len(streetName) >= 1):
				if (streetName[0] == " "):
					streetName = streetName[1: len(landData['Address']) - 1]

			for locationData in repo["klovett.bbLocations"].find():

				if (streetName in locationData["description"]):
					modifiedPiece = {"street name": streetName, "Location": locationData["Location"]}
					modifiedDictionary.append(modifiedPiece)
					print("Progress Made on second transformation... Current time: ", datetime.datetime.now())

		repo['klovett.streetCoordinates'].insert_many(modifiedDictionary)
		
		repo.logout()
		
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
			Create the provenance document describing everything happening
			in this script. Each run of the script will generate a new
			document describing that invocation event.
		'''
		
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('klovett', 'klovett')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.boston.gov/')
		doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/')
		doc.add_namespace('cdp', 'https://data.cambridgema.gov/')

		this_script = doc.agent('alg:klovett#transformationTwo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		modifiedResource = doc.entity('dat:transform2', {'prov:label':'Transformation Two', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		bbLocResource = doc.entity('bdp:15e7fa44-b9a8-42da-82e1-304e43460095', {'prov:label':'BB Loc Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		bostonResource = doc.entity('bdp:5b027436-5213-4be6-ab5f-485a03f74500', {'prov:label':'Boston Land', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_bbLocations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_landBoston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_modifiedResource = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_modifiedResource, this_script)
		doc.wasAssociatedWith(get_landBoston, this_script)
		doc.wasAssociatedWith(get_bbLocations, this_script)

		doc.usage(get_bbLocations, bbLocResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Computation',
				'ont:Query':'?type=BB+Locations&$select=description'
				}
				)
		doc.usage(get_landBoston, bostonResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Retrieval',
				'ont:Query':'?type=BB+Alerts&$select=Address'
				}
				)
		doc.usage(get_modifiedResource, modifiedResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Computation',
				'ont:Query':'?type=BB+Alerts&$select=street name, Location'
				}
				)	
		bbLocations = doc.entity('dat:klovett#bbLocations', {prov.model.PROV_LABEL:'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(bbLocations, this_script)
		doc.wasGeneratedBy(bbLocations, get_bbLocations, endTime)
		doc.wasDerivedFrom(bbLocations, modifiedResource, get_bbLocations, get_bbLocations, get_bbLocations)

		landBoston = doc.entity('dat:klovett#landBoston', {prov.model.PROV_LABEL:'Boston Land', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(landBoston, this_script)
		doc.wasGeneratedBy(landBoston, get_landBoston, endTime)
		doc.wasDerivedFrom(landBoston, modifiedResource, get_landBoston, get_landBoston, get_landBoston)

		modified = doc.entity('dat:klovett#transform2', {prov.model.PROV_LABEL:'Second Transform', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(modified, this_script)
		doc.wasGeneratedBy(modified, get_modifiedResource, endTime)
		doc.wasDerivedFrom(modified, modifiedResource, get_modifiedResource, get_modifiedResource, get_modifiedResource)

		repo.logout()
		
		return doc