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
import numpy as np
from area import area



def Clusts(clustnum,labels_array):
    return (np.where(labels_array == clustnum)[0])

class transformation4(dml.Algorithm):
    contributor = 'htw93_tscheung_wenjun'
    reads = ['htw93_tscheung_wenjun.MBTAStops','htw93_tscheung_wenjun.BostonFood','htw93_tscheung_wenjun.BostonGarden']
    writes = ['htw93_tscheung_wenjun.BostonHotelCorrelation']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')

        BostonMbta = repo.htw93_tscheung_wenjun.MBTAStops
        BostonFood = repo.htw93_tscheung_wenjun.BostonFood
        BostonGarden = repo.htw93_tscheung_wenjun.BostonGarden
        #BostonHotelRatingOriginal = repo.htw93_tscheung_wenjun.BostonHotel
        #HotelRaing = BostonHotelRatingOriginal.find()
        mbta = BostonMbta.find()
        food = BostonFood.find()
        garden = BostonGarden.find()

        mbta_list=[]
        food_list=[]
        garden_list=[]
        
        for entry in mbta:
            #print(entry)
            [lat,lon] = entry['location']
            coord = [lat,lon]
            #print (coord)
            #print(string.split(','))
            mbta_list.append(coord)

        for entry in food:
            #print(entry)
            [lat,lon] = entry['location']
            coord = [lat,lon]
            #print (coord)
            #print(string.split(','))
            food_list.append(coord)

        for entry in garden:
            #print(entry)
            [lat,lon] = entry['location']
            coord = [lat,lon]
            #print (coord)
            #print(string.split(','))
            garden_list.append(coord)
        

        X = np.array(mbta_list)
        kmeans = KMeans(n_clusters=10, random_state=0).fit(X)
        kmeans_arr_mbta = kmeans.cluster_centers_

        ids = []
        for i in range(10):
            ids.append(Clusts(i,kmeans.labels_).tolist())

        ids = [len(i) for i in ids]
        max_index_mbta = ids.index(max(ids))
        print (max_index_mbta)
        print(kmeans_arr_mbta[max_index_mbta])

        
    
        X = np.array(food_list)
        kmeans = KMeans(n_clusters=10, random_state=0).fit(X)
        kmeans_arr_food = kmeans.cluster_centers_
        print(kmeans_arr_food)

        ids = []
        for i in range(10):
            ids.append(Clusts(i,kmeans.labels_).tolist())
        
        ids = [len(i) for i in ids]
        max_index_food = ids.index(max(ids))
        print (max_index_food)
        print(kmeans_arr_food[max_index_food])

        X = np.array(garden_list)
        kmeans = KMeans(n_clusters=10, random_state=0).fit(X)
        kmeans_arr_garden = kmeans.cluster_centers_

        ids = []
        for i in range(10):
            ids.append(Clusts(i,kmeans.labels_).tolist())

        ids = [len(i) for i in ids]
        max_index_garden = ids.index(max(ids))
        print (max_index_garden)
        print(kmeans_arr_garden[max_index_garden])

        print('Finished creating collection htw93_tscheung_wenjun.BostonHotelCorrelation')
        
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

        pass

transformation4.execute()
#doc = transformation4.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
