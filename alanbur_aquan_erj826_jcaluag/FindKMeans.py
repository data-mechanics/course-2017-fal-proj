"""
CS591
Project 2
11.12.17
getKmeansNY.py
"""
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import math
import numpy as np
from sklearn.cluster import KMeans
import random

#prevents deprecation warnings, a known and fixed issue
#(see https://stackoverflow.com/questions/36892390/deprecationwarning-in-sklearn-minibatchkmeans)
import warnings 
warnings.filterwarnings("ignore", category=DeprecationWarning)

#the largest acceptable distance between two data 
acceptableDistance = 0.05 #this is about 3 miles from degree long//lat conversion to miles
coordinates = []

#takes all the coordinates and the kmeans cluster and returns the highest distance between
#any point and its respective centroid    
def getMaxDistance(kmeans, coordinates):
    maxDistance = 0
    for i in range(len(coordinates)):
        clusterCenter =kmeans.predict(coordinates[i])
        current = distance(kmeans.cluster_centers_[clusterCenter][0],coordinates[i])
        if(current > maxDistance):
            maxDistance = current
    return maxDistance

def getAvgDistance(kmeans, coordinates):
    totalDistance = 0
    for i in range(len(coordinates)):
        clusterCenter = kmeans.predict([coordinates[i]])
        totalDistance += distance(kmeans.cluster_centers_[clusterCenter][0],coordinates[i])
    return totalDistance/len(coordinates)
        
#returns the distance between two long/lat points
def distance(p0, p1):
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)

#our algorithm
class FindKMeans(dml.Algorithm):
    contributor = 'alanbur_aquan_erj826_jcaluag'
    reads = ['alanbur_aquan_erj826_jcaluag.parseNYaccidents']
    writes = ['alanbur_aquan_erj826_jcaluag.kMeansNY']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()
        print('Finding optimal number of means')
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')          
        repo.dropCollection("alanbur_aquan_erj826_jcaluag.kMeansNY")
        repo.createCollection("alanbur_aquan_erj826_jcaluag.kMeansNY")

        #get coordinates from colleciton
        collection = repo.alanbur_aquan_erj826_jcaluag.parseNYaccidents
        coordinates = []

        for entry in collection.find():    
            try: #make the array for kmeans
                datapoint = [entry['longitude'],entry['latitude']]
                coordinates.append(datapoint)        
            except:
                continue   

        SampleSize=100
        if trial:
            TrialSample=coordinates[:SampleSize]
            for i in range(SampleSize+1,len(coordinates)):
                j=random.randint(1,i)
                if j<SampleSize:
                    TrialSample[j] = coordinates[i]
            print('Running in trial mode')
            coordinates=TrialSample
            print(coordinates)

        X = np.array(coordinates)
 
        #True means find with averages, False means find with maxes
        avgOrMaxDistToggle = True

        #run kmeans with two clusters for starting point
        kmeans = KMeans(n_clusters=2, random_state=0).fit(X)
        
        if(avgOrMaxDistToggle):
            metric = getAvgDistance(kmeans,coordinates)
        else:
            metric = getMaxDistance(kmeans,coordinates)
        clusters = 2
        
        #run kmeans while acceptableDistance is not fulfilled
        while(metric > acceptableDistance):
            #print("the metric is: " + str(metric))
            clusters+=1
            kmeans = KMeans(n_clusters=clusters, random_state=0).fit(X) 
            if(avgOrMaxDistToggle):
                metric = getAvgDistance(kmeans,coordinates)
            else:
                metric = getMaxDistance(kmeans,coordinates)
        print("we're done! the " +  str({True: "avg", False: "max"} [avgOrMaxDistToggle])+ " distance is: " + str(metric) + " at " + str(clusters) + " clusters!")
    
        #plug into the centroids into dictionary for returning
        n={}
        centroids = kmeans.cluster_centers_
        for i in range(len(centroids)):
            n[str(i)]=centroids[i].tolist()
        
        repo['alanbur_aquan_erj826_jcaluag.kMeansNY'].insert(n, check_keys=False)
        repo['alanbur_aquan_erj826_jcaluag.kMeansNY'].metadata({'complete':True})
        print(repo['alanbur_aquan_erj826_jcaluag.kMeansNY'].metadata())
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

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        #resources:
        
        #define the agent
        this_script = doc.agent('alg:alanbur_aquan_erj826_jcaluag#FindKMeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        
        #define the agent
        this_script = doc.agent('alg:alanbur_aquan_erj826_jcaluag#FindKMeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        
        #define the input resource
        resource = doc.entity('dat:parseNYaccidents', {'prov:label':'NY Parsed Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #define the activity of taking in the resource
        action = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(action, this_script)
        doc.usage(action, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        
        
        #define the writeout 
        output = doc.entity('dat:alanbur_aquan_erj826_jcaluag#kMeansNY', {prov.model.PROV_LABEL:'NY KMeans', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, action, endTime)
        doc.wasDerivedFrom(output, resource, action, action, action)

        repo.logout()
                  
        return doc





FindKMeans.execute(False)

## eof
