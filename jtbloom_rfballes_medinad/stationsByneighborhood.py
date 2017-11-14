import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geoql import geoql 
from bson import ObjectId
from bson.json_util import dumps


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

		#get hubway stations data  
		s = repo.jtbloom_rfballes_medinad.boston_hubway_stations.find()
		print(s)
		#l = list(s)
		#print(l)
		#stations = dumps(s)
		#print(stations)
		
		
		#stations_list = [{"Station":x ["Station"], "latitude":x ["Latitude"], "longitude":x ["Longitude"]} for x in s]
		
		stations = geoql.loads(stations)

		


		#get neighborhoods
		#neighborhood_shapes = geoql.loads(repo.jtbloom_rfballes_medinad.neighborhoods.find())

		#print('HI')
		


stationsByneighborhood.execute()