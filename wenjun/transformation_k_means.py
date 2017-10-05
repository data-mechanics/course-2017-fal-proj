import urllib.request
#import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from sklearn.cluster import KMeans
import numpy as np

class transformation_k_means(dml.Algorithm):
    contributor = 'wenjun'
    reads =[]
    writes = ['wenjun.allParkingMeters','wenjun.parkingMetersBoston','k_means_parkingMeters']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wenjun','wenjun')

        allParkingMeters = repo['wenjun.parkingMetersBoston']

        allLocation_list=[]

        for entry in allParkingMeters.find():
            #print(entry)
            [lon,lat] = entry['geometry']['coordinates']
            coord = [lon,lat]
            #print (coord)
            #print(string.split(','))
            allLocation_list.append(coord)

        

        
        X = np.array(allLocation_list)

        kmeans = KMeans(n_clusters=100, random_state=0).fit(X)
        kmeans_arr = kmeans.cluster_centers_
        kmeans_list=[]
        count =0
        for entry in kmeans_arr:
            kmeans_list.append({'k_means_label':count, 'coordinates' : entry.tolist()})
            count=count+1
        #print(kmeans_list)

        repo.dropCollection('k_means_parkingMeters')
        repo.createCollection('k_means_parkingMeters')
        repo['wenjun.k_means_parkingMeters'].insert_many(kmeans_list)
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wenjun', 'wenjun')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset')
        #doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        this_script = doc.agent('alg:wenjun#transformation_k_mans',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_k_means_ParkingMeters = doc.entity('dat:wenjun#k_means_ParkingMeters',
                                               {'prov:label': 'k_means_ParkingMeters',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

       # Activities' Associations with Agent
        transform_k_means= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "transform k means",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(transform_k_means, this_script)

        # Record which activity used which resource
        doc.usage(transform_k_means, resource_k_means_ParkingMeters, startTime)

        # Result dataset entity
        k_means_ParkingMeters  = doc.entity('dat:wenjun#k_means_ParkingMeters',
                                       {prov.model.PROV_LABEL: 'k means Parking Meters Restaurants',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(k_means_ParkingMeters, this_script)
        doc.wasGeneratedBy(k_means_ParkingMeters, transform_k_means, endTime)
        doc.wasDerivedFrom(k_means_ParkingMeters, resource_k_means_ParkingMeters, transform_k_means, transform_k_means,
                           transform_k_means)


        repo.logout()
        return doc        

        
        
 

#transformation_k_means.execute()

