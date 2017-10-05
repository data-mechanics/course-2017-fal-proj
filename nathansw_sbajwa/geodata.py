import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from urllib.request import urlopen
import os

class geodata(dml.Algorithm):

	contributor = 'nathansw_sbajwa'
	reads = ['nathansw_sbajwa.mbta']
	writes = ['nathansw_sbajwa.geodata']

	@staticmethod
	def execute(trial = False):

		# directory navigation
		curr_dir = os.getcwd()
		new_dir = curr_dir + "\\nathansw_sbajwa\\"
		# would like to eventually elimate in favor of direct db draws from mbta -> geodata
		temp_f = open(new_dir + 'geo_coords.txt', 'r')
		loc_coords = temp_f.readlines()


		startTime = datetime.datetime.now()

		# open db client and authenticate
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa','nathansw_sbajwa')

		# initilize variables
		base_url = "https://azure.geodataservice.net/GeoDataService.svc/GetUSDemographics?"
		form = "&$format=json"
		data = {}

		for entry in loc_coords:
			coords = entry.strip('\n')
			sep_coords = coords.split(', ')

			lon = "longitude=" + (sep_coords[1].strip(']')).strip("'")
			lat = "&latitude=" + (sep_coords[0].strip('[')).strip("'")

			# build url
			url = base_url + lon + lat + form

			temp = json.loads(urlopen(url).read().decode('utf-8'))

			first_key = lat.strip("&latitude=")
			second_key = lon.strip("longitude=")

			# replaces '.' with '+' due to mongodb restrictions
			key_name = "(" + first_key.replace('.','+') + "," + second_key.replace('.','+') + ")"

			data[key_name] = temp.pop('d'[0])

		s = json.dumps(data, indent=4)
		repo.dropCollection("geodata")
		repo.createCollection("geodata")
		repo['nathansw_sbajwa.geodata'].insert_one(data)

		repo.logout()
		
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("nathansw_sbajwa","nathansw_sbajwa")
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nathansw_sbajwa/') # The scripts in / format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/nathansw_sbajwa/') # The data sets in / format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
	
		doc.add_namespace('geodata', 'http://azure.geodataservice.net/GeoDataService.svc/')

		this_script = doc.agent('alg:nathansw_sbajwa#geodata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		get_geodata = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_geodata, this_script)

		resource = doc.entity('geodata:GetUSDemographics', {'prov:label':'Lifestyle Data By Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		doc.usage(get_geodata, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})

		geodata = doc.entity('dat:nathansw_sbajwa#geodata', {prov.model.PROV_LABEL:'Lifestyle Data by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(geodata, this_script)
		doc.wasGeneratedBy(geodata, get_geodata, endTime)
		doc.wasDerivedFrom(geodata, resource, get_geodata, get_geodata, get_geodata)			

		repo.logout()

		return doc

# geodata.execute()
# doc = geodata.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))