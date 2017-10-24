import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transformationThree(dml.Algorithm):
	contributor = 'klovett'
	reads = ['klovett.landCambridge', 'klovett.landBoston']
	writes = ['klovett.officeLocs']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('klovett', 'klovett')
		repo.dropCollection("officeLocs")
		repo.createCollection("officeLocs")

		modifiedDictionary = []

		for landDataBoston in repo["klovett.landBoston"].find():

			bostonPropUse = landDataBoston['Property Type']

			if (bostonPropUse == "Office"):
				modifiedPiece = {"City": "Boston", "Location": landDataBoston["Address"]}
				modifiedDictionary.append(modifiedPiece)
				print("Progress Made on third transformation... Current time: ", datetime.datetime.now())

		for landDataCambridge in repo["klovett.landCambridge"].find():

			cambridgePropUse = landDataCambridge['land_use_category']

			if (cambridgePropUse == "Office"):
				modifiedPiece = {"City": "Cambridge", "Location": landDataCambridge["location"]}
				modifiedDictionary.append(modifiedPiece)
				print("Progress Made on third transformation... Current time: ", datetime.datetime.now())

		repo['klovett.officeLocs'].insert_many(modifiedDictionary)

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

		this_script = doc.agent('alg:klovett#transformationThree', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		modifiedResource = doc.entity('dat:transform3', {'prov:label':'Transformation Three', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		bostonResource = doc.entity('bdp:5b027436-5213-4be6-ab5f-485a03f74500', {'prov:label':'Boston Land', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		cambridgeResource = doc.entity('cdp:ufnx-m9uc', {'prov:label':'Cambridge Land', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		get_landBoston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_landCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_modifiedResource = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_landBoston, this_script)
		doc.wasAssociatedWith(get_landCambridge, this_script)
		doc.wasAssociatedWith(get_modifiedResource, this_script)

		doc.usage(get_landBoston, bostonResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Computation',
				'ont:Query':'?type=BB+Locations&$select=Property Type'
				}
				)
		doc.usage(get_landCambridge, cambridgeResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Retrieval',
				'ont:Query':'?type=BB+Alerts&$select=land_use_category'
				}
				)
		doc.usage(get_modifiedResource, modifiedResource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Retrieval',
				'ont:Query':'?type=BB+Alerts&$select=Cambridge, Location'
				}
				)
		
		landBoston = doc.entity('dat:klovett#landBoston', {prov.model.PROV_LABEL:'Boston Land', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(landBoston, this_script)
		doc.wasGeneratedBy(landBoston, get_landBoston, endTime)
		doc.wasDerivedFrom(landBoston, modifiedResource, get_landBoston, get_landBoston, get_landBoston)

		landCambridge = doc.entity('dat:klovett#landCambridge', {prov.model.PROV_LABEL:'Cambridge Land', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(landCambridge, this_script)
		doc.wasGeneratedBy(landCambridge, get_landCambridge, endTime)
		doc.wasDerivedFrom(landCambridge, modifiedResource, get_landCambridge, get_landCambridge, get_landCambridge)

		modified = doc.entity('dat:klovett#transform3', {prov.model.PROV_LABEL:'Third Transform', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(modified, this_script)
		doc.wasGeneratedBy(modified, get_modifiedResource, endTime)
		doc.wasDerivedFrom(modified, modifiedResource, get_modifiedResource, get_modifiedResource, get_modifiedResource)

		repo.logout()
		
		return doc