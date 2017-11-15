import urllib.request
import json
import geojson
import dml
import prov.model
import datetime
import uuid
import shapely 
from shapely.geometry import shape, Point



class stationsByneighborhood(object):
	"""docstring fos stationsByneighborhood"""
	contributor = 'jtbloom_rfballes_medinad'
	reads = ['jtbloom_rfballes_medinad.boston_hubway_stations', 'jtbloom_rfballes_medinad.neighborhoods']
	writes = ['jtbloom_rfballes_medinad.stationsByneighborhood']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')	

		repo.dropCollection('jtbloom_rfballes_medinad.stationsByneighborhood')
		repo.createCollection('jtbloom_rfballes_medinad.stationsByneighborhood')
		
		neighborhoods = list(repo['jtbloom_rfballes_medinad.neighborhoods'].find())

		#hubway stations locations
		stations = list(repo['jtbloom_rfballes_medinad.boston_hubway_stations'].find())

		new_set = []
		for polygon in neighborhoods:
			figure = shape(polygon['Geo Shape'])
			for item in stations: 
				point = Point(item['Longitude'], item['Latitude'])
				name = item['Station']
				content = figure.contains(point)
				if content == True:
					new_set.append({"Neighborhood": polygon["Neighborhood"], "Station": item['Station'], "Docks": item['Number of Docks']})

		#print(new_set)
		#print(len(new_set))

		repo['jtbloom_rfballes_medinad.stationsByneighborhood'].insert_many(new_set)

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

				
stationsByneighborhood.execute()