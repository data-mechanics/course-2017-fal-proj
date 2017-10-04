import json
import datetime
import dml
import prov.model
import uuid
import sys
import pandas as pd
from pprint import pprint
from bson import ObjectId

### This transformation combines the MBTA and Geodata dataset
###
### Relevant MBTA columns: distance, stop_id
###
### Relevant Geodata columns: Total number of employees, total number of businesses,
### average house value, total housing units, total occupied units 
###
### Relationship being explored: Given the coordinates of different locations within 
### neighborhoods of Boston, how accessible is the MBTA to the people (specfically 
### those in the workforc) living there? Is there any further correlation between the 
### amount of vacant housing units compared to the average unit value in that location?

class mbta_versus_geodata(dml.Algorithm):

	contributor = 'nathansw_sbajwa'
	reads = ['nathansw_sbajwa.mbta', 'nathansw_sbajwa.geodata']
	writes = ['nathansw_sbajwa.mbta_verses_geodata']

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		# open db client and authenticate
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa', 'nathansw_sbajwa')

		################## MBTA #################### 

		mbta_db = repo['nathansw_sbajwa.mbta']
		stops = mbta_db.find_one()
		stops_data = {}

		# Each coordinate produces x amount of stops that fall within a 1 mile radius.
		#
		# For coordinates, keep track of the distance to the closest stop, farthest stop, 
		# and the average distance to all the stop. Additionally, count the amount of stops
		# that fall into the 1 mile radius
		for coords in stops:
			distances = []
			stop_id = []
			if coords == "_id": 
				continue
			stops_data[coords] = {}

			for entry in stops[coords]:
				distances.append(float(entry.get('distance')))
				stop_id.append(entry.get('stop_id')) 	# stop_id is unique and will help us count how many stops per coordinate
			
			if len(distances) == 0:
				stops_data[coords]['Closest Stop'] = 0
				stops_data[coords]['Farthest Stop'] = 0
				stops_data[coords]['Average Distance'] = 0
				stops_data[coords]['Stops within 1 mi Radius'] = 0
				continue

			stops_data[coords]['Closest Stop'] = min(distances)
			stops_data[coords]['Farthest Stop'] = max(distances)
			average_dist = sum(distances)/float(len(distances))
			stops_data[coords]['Average Distance'] = average_dist

			count = len(set(stop_id))	# set ensures that we are only counting unique stops for each coordinate
			stops_data[coords]['Stops within 1 mi Radius'] = count

		stops_data = json.dumps(stops_data, indent=4)	#string object
		json_stops = json.loads(stops_data)				#json/dict object

		################ GEODATA ########################

		geodata_db = repo['nathansw_sbajwa.geodata']
		geo = geodata_db.find_one()

		geo_cols = ['TotalNumberEmployees', 'TotalNumberOfBusinesses', 'AverageHouseValue', \
		'TotalHousingUnits', 'OccupiedHousingUnits']
		geo_data = {}

		# Grab the data for each entry in geodata at the specified column and add it to geo_data{}
		# as a float so we can do math operations with the values later
		for coords in geo:
			if coords == "_id":
				continue
			geo_data[coords] = {}
			for entry in geo[coords]:
				geo_data[coords]['Employee Count'] = float(entry.get(geo_cols[0]))
				geo_data[coords]['Businesses Count'] = float(entry.get(geo_cols[1]))
				geo_data[coords]['Average House Value'] = float(entry.get(geo_cols[2]))
				geo_data[coords]['Total Housing Units'] = float(entry.get(geo_cols[3]))
				geo_data[coords]['Total Occupied Units'] = float(entry.get(geo_cols[4]))

		geo_data = json.dumps(geo_data, indent=4)
		json_geo = json.loads(geo_data)

		################################################

		mbta_df = pd.DataFrame(json_stops)
		geodata_df = pd.DataFrame(json_geo)
		# merge the two data sets together with an outer join 
		frames = [mbta_df, geodata_df]
		result = pd.concat(frames, join='outer')
		# switch the rows and columns
		# original setup: columns = neighborhoods
		# new setup: rows = neighborhoods 
		## allowing the different categories of information to be columns will make it easier to query and search for what we want
		result = pd.DataFrame.transpose(result)

		### find percentage of vacant units and create a new column
		result['Percentage Vacant Units'] = (1 - (result['Total Occupied Units'] / result['Total Housing Units'])) * 100

		## write to csv for testing purposes
		# result.to_csv('algorithm2.csv', encoding='utf-8')

		test = result.to_dict('index')
		agg_data = json.dumps(test, indent=4)
		merged_data = json.loads(agg_data)

		repo.dropCollection('mbta_versus_geodata')
		repo.createCollection('mbta_versus_geodata')
		repo['nathansw_sbajwa.mbta_versus_geodata'].insert_one(merged_data)

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return doc