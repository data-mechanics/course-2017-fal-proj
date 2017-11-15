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
	reads = ['jtbloom_rfballes_medinad.outgoing_trips', 'jtbloom_rfballes_medinad.neighborhoods']
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
		station_trips = list(repo['jtbloom_rfballes_medinad.outgoing_trips'].find())

		newset = []
		for polygon in neighborhoods:
			figure = shape(polygon['Geo Shape'])
			for item in station_trips:
				point = Point(float(item['longitude']), float(item['latitude']))
				content = figure.contains(point)
				if content == True:
					newset.append({'Neighborhood':polygon['Neighborhood'], 'station':item['station'], 'trips':item['# outgoing trips']})


		

		repo['jtbloom_rfballes_medinad.stationsByneighborhood'].insert_many(newset) #dont delete 

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

				
stationsByneighborhood.execute()