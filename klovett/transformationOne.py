import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transformationOne(dml.Algorithm):
	contributor = 'klovett'
	reads = ['klovett.bbLocations', 'klovett.bbAlerts']
	writes = ['klovett.bbLocationsTransformed']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('klovett', 'klovett')
		repo.dropCollection("bbLocationsTransformed")
		repo.createCollection("bbLocationsTransformed")

		modifiedDictionary = []

		for locationData in repo["klovett.bbLocations"].find():
			for alertData in repo["klovett.bbAlerts"].find():
				if (locationData["description"] == alertData["description"]):
					modifiedPiece = {"description":locationData["description"], "Location":locationData["Location"], "timestamp":alertData["timestamp"], "fullness":alertData["fullness"], "collection":alertData["collection"]}
					modifiedDictionary.append(modifiedPiece)
					print("Progress Made on first transformation... Current time: ", datetime.datetime.now())

		repo['klovett.bbLocationsTransformed'].insert_many(modifiedDictionary)

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

		this_script = doc.agent('alg:klovett#transformationOne', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		modifiedResource = doc.entity('dat:transform1', {'prov:label':'Transformation One', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		bbLocResource = doc.entity('bdp:15e7fa44-b9a8-42da-82e1-304e43460095', {'prov:label':'BB Loc Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		bbAlertResource = doc.entity('bdp:c8c54c49-3097-40fc-b3f2-c9508b8d393a', {'prov:label':'BB Alerts Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_bbAlertResource = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_bbLocResource = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_modifiedResource = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_modifiedResource, this_script)
		doc.wasAssociatedWith(get_bbLocResource, this_script)
		doc.wasAssociatedWith(get_bbAlertResource, this_script)

		doc.usage(get_bbLocResource, bbLocResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Computation',
				'ont:Query':'?type=BB+Locations&$select=description, Location'
				}
				)
		doc.usage(get_bbAlertResource, bbAlertResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Computation',
				'ont:Query':'?type=BB+Alerts&$select=description, fullness, Location, timestamp, collection'
				}
				)
		doc.usage(get_modifiedResource, modifiedResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Computation',
				'ont:Query':'?type=BB+Alerts&$select=description, fullness, Location, timestamp, collection'
				}
				)	
		bbLocations = doc.entity('dat:klovett#bbLocations', {prov.model.PROV_LABEL:'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(bbLocations, this_script)
		doc.wasGeneratedBy(bbLocations, get_bbLocResource, endTime)
		doc.wasDerivedFrom(bbLocations, modifiedResource, get_bbLocResource, get_bbLocResource, get_bbLocResource)

		bbAlerts = doc.entity('dat:klovett#bbAlerts', {prov.model.PROV_LABEL:'Big Belly Alerts', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(bbAlerts, this_script)
		doc.wasGeneratedBy(bbAlerts, get_bbAlertResource, endTime)
		doc.wasDerivedFrom(bbAlerts, bbAlertResource, get_bbAlertResource, get_bbAlertResource, get_bbAlertResource)

		modified = doc.entity('dat:klovett#transform1', {prov.model.PROV_LABEL:'First Transform', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(modified, this_script)
		doc.wasGeneratedBy(modified, get_modifiedResource, endTime)
		doc.wasDerivedFrom(modified, modifiedResource, get_modifiedResource, get_modifiedResource, get_modifiedResource)

		repo.logout()
		
		return doc