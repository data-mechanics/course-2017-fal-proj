import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty
import scipy.stats
from sklearn.cluster import KMeans
from sklearn import preprocessing
import numpy as np
import matplotlib.pyplot as plt
import itertools


from transformation4_parameters import transformation4_parameters

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')
repo.dropCollection("BostonHotelPotentialPermutation")
repo.createCollection("BostonHotelPotentialPermutation")



permutation_list = list(itertools.permutations([0, 1, 2, 3]))

for entry in permutation_list:
    origin = entry[0]
    garden = entry[1]
    food = entry[2]
    mbta = entry[3]
    print(origin,garden,food,mbta)
    transformation4_parameters.execute(origin,garden,food, mbta)
    # doc = transformation4_parameters.provenance()
    #print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
