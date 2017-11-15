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
		
		#boston neighborhood shapes
		nei_url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
		nei_url = urllib.request.urlopen(nei_url).read().decode("utf-8")
		neighborhoods = geojson.loads(nei_url)

		#hubway stations locations
		stations_url = 'https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/download/?format=geojson&timezone=America/New_York'
		stations_url = urllib.request.urlopen(stations_url).read().decode("utf-8")
		stations = geojson.loads(stations_url)

		print(stations)
		

		#print(neighborhoods)

		# n_list = []
		# for item in neighborhoods["features"]:
		#  	print(item)
		#  	fts = {}
		#  	fts["features"] = item
		#  	n_list.append(fts)


		# s = stations.properties_null_remove()\
		# 			.tags_parse_str_to_dict()
		# #print(n_list[0])

		# for i in  n_list:
		# 	#print(type(i))
		# 	k = geojson.dumps(i)
		# 	k = k.encode("utf-8")
		# 	k = k.decode("utf-8")
		# 	k = geoql.loads(k)
		# 	print(type(k))
		# 	s = s.keep_that_intersect(k)
		
		#for i in n_list:
			#print(type(i))
			#k = geojson.loads(str(i))
			#k  = geoql.loads(k)
			#print(type(k))
			
		#s.dump(open('test.geojson', 'w'))
		#open('leaflet.html', 'w').write(geoleaflet.html(s)) # Create visualization.
		#open('leaflet_n.html', 'w').write(geoleaflet.html(neighborhoods))

		#print(stations)



		#print(x)
		#stations = dumps(s)
		#stations = loads(stations)
		#print(stations)
		#l = loads(s)
		#print(l)
		#l = list(s)
		#print(l)
		#stations = dumps(s)
		#print(stations)
		
		
		#stations_list = [{"Station":x ["Station"], "latitude":x ["Latitude"], "longitude":x ["Longitude"]} for x in s]
		
		#stations = geoql.loads(stations)

		


		#get neighborhoods
		#neighborhood_shapes = geoql.loads(repo.jtbloom_rfballes_medinad.neighborhoods.find())

		#print('HI')
		


stationsByneighborhood.execute()