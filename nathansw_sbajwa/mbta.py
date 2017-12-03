import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import os
from urllib.request import urlopen
from uszipcode import ZipcodeSearchEngine

class mbta(dml.Algorithm):

	contributor = 'nathansw_sbajwa'
	reads = []
	writes = ['nathansw_sbajwa.mbta']

	@staticmethod
	def execute(trial = False):

		# directory navigation
		curr_dir = os.getcwd()
		new_dir = curr_dir + "\\nathansw_sbajwa\\"

		####################################################
		###

		# text file to hold all longitude and latitude coordinates that are collected
		# would like to eventually elimate in favor of direct db draws from mbta -> geodata
		f = open(new_dir + 'geo_coords.txt', 'w')

		###
		####################################################

		startTime = datetime.datetime.now()

		# open db client and authenticate
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa','nathansw_sbajwa')

		# central coordinates for neighborhoods in Boston
		locs = {'Allston':[42.3539038, -71.1337112], 'Back Bay':[42.3475975, -71.0753291], \
		'Beacon Hill':[42.3588, -71.0707], 'Brighton':[42.3464, -71.1627], \
		'Charlestown':[42.3782, -71.0602], 'Dorchester':[42.3016, -71.0676], \
		'Downtown':[42.361145, -71.057083], 'East Boston':[42.3702, -71.0389], \
		'Fenway':[42.3429, -71.1003], 'Harbor Islands':[25.8876, -80.1312], \
		'Hyde Park':[42.2565, -71.1241], 'Jamaica Plain':[42.3097, -71.1151], \
		'Longwood':[42.3427, -71.1099], 'Mattapan':[42.2771, -71.0914], \
		'Mission Hill':[42.3296,-71.1062], 'North End':[42.3647, -71.0542], \
		'Roslindale':[42.2832, -71.1270], 'Roxbury':[42.3601, -71.0589], \
		'South Boston':[42.3381, -71.0476], 'South Boston Waterfront':[42.364506, -71.038887], \
		'South End':[42.3388, -71.0765], 'West End':[42.3644, -71.0661], \
		'West Roxbury':[42.2798,-71.1627]}

		# url = "http://realtime.mbta.com/developer/api/v2/stopsbylocation?\
		# api_key=TYDhtqF81Ua27IfG2lXEqA&lat=" + x + "&lon=" + y + "&format=json"
		url_base = "http://realtime.mbta.com/developer/api/v2/stopsbylocation?"
		api_key = 'api_key=' + dml.auth['services']['MBTADevelopmentPortal']['key']
		form = "&format=json"
		data = {}

		search = ZipcodeSearchEngine()

		# get all other zip codes in Boston
		zips = [("0" + str(x)) for x in range(2108, 2138)]
		others = ['02163', '02196', '02199', '02201', '02203', '02204', '02205', '02206', '02210', '02211', '02212', '02215',\
		 '02217', '02222', '02126', '02228', '02241', '02266', '02283', '02284', '02293', '02295', '02297', '02298', '02467']
		zips += others

		coords = []

		# zipcode module returns three different pairs of coordinates that encompass given zipcode
		for i in zips:
			zipcode = search.by_zipcode(i)
			lat = str(zipcode['Latitude'])
			lon = str(zipcode['Longitude'])
			NElat = str(zipcode['NEBoundLatitude'])
			NElon = str(zipcode['NEBoundLongitude'])
			SWlat = str(zipcode['SWBoundLatitude'])
			SWlon = str(zipcode['SWBoungLongitude'])
			if (lat == 'None') or (lon == 'None'):
				continue
			coords.append([lat,lon])
			coords.append([NElat,NElon])
			coords.append([SWlat,SWlon])

		for town, points in locs.items():
			lat = str(points[0])
			lon = str(points[1])
			coords.append([lat,lon])

		for loc in coords:

			#################################################################################
			###

			# write all coordinates collected to a text file separated by a newline
			# would like to eventually elimate in favor of direct db draws from mbta -> geodata
			f.write(str(loc) + "\n")

			###
			##################################################################################

			# create url to make API call to MBTA portal
			lat = "&lat=" + str(loc[0])
			lon = "&lon=" + str(loc[1])
			url = url_base + api_key + lat + lon + form
			temp = json.loads(urlopen(url).read().decode('utf-8'))

		    # makes every key in json dictionary the coordinates
		    # with '.' char replaced with '+' due to mongodb restrictions
			loc0 = str(loc[0]).replace('.','+')
			loc1 = str(loc[1]).replace('.','+')

			key_name = "(" + loc0 + "," + loc1 + ")"
			data[key_name] = temp.pop('stop')

		##########
		###

		f.close()

		###
		##########

		s = json.dumps(data, indent=4)
		repo.dropCollection("mbta")
		repo.createCollection("mbta")
		repo['nathansw_sbajwa.mbta'].insert_one(data)

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("nathansw_sbajwa","nathansw_sbajwa")
		
		## Namespaces
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nathansw_sbajwa/') # The scripts in / format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/nathansw_sbajwa/') # The data sets in / format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
	
		doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

		## Agents
		this_script = doc.agent('alg:nathansw_sbajwa#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		## Activities
		get_mbta = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		## Entitites
		# Data Source
		resource = doc.entity('mbta:stopsbylocation', {'prov:label':'MBTA Stops By Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		# Data Generated
		mbta = doc.entity('dat:nathansw_sbajwa#mbta', {prov.model.PROV_LABEL:'MBTA Stops',prov.model.PROV_TYPE:'ont:DataSet'})

		## wasAssociatedWith
		doc.wasAssociatedWith(get_mbta, this_script)

		## used
		doc.usage(get_mbta, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

		## wasGeneratedBy
		doc.wasGeneratedBy(mbta, get_mbta, endTime)

		## wasAttributedTo
		doc.wasAttributedTo(mbta, this_script)

		## wasDerivedFrom
		doc.wasDerivedFrom(mbta, resource, get_mbta, get_mbta, get_mbta)		

		repo.logout()

		return doc

# mbta.execute()
# doc = mbta.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))