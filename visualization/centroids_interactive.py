import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import sys
import math
import numpy as np
import pdb


def find_centroids(K):
    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
    db = client.repo
    collection = db['bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds']

    # Set up the lat and long coordinates
    building_list = []
    for building in collection.find():
        if building['LATITUDE'] == '#N/A' or building['LONGITUDE'] == '#N/A':
            pass
        else:
            building_list.append( [float(building['LATITUDE']), float(building['LONGITUDE'])] )

    # Build kmeans
    kmeans = KMeans(n_clusters=K)
    kmeans.fit(building_list)
    
    # Build the input list (kmean_list) and the output list (closest_buildings_to_centroids)
    kmean_list = kmeans.cluster_centers_.tolist()

    return kmean_list
