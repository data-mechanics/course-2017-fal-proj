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
#the largest acceptable distance between two data 
acceptableDistance = 0.05 #this is about 3 miles from degree long//lat conversion to miles
coordinates = []

#takes all the coordinates and the kmeans cluster and returns the highest distance between
#any point and its respective centroid    
def getMaxDistance(kmeans):
    maxDistance = 0
    for i in range(len(coordinates)):
        clusterCenter =kmeans.predict(coordinates[i])
        current = distance(kmeans.cluster_centers_[clusterCenter][0],coordinates[i])
        if(current > maxDistance):
            maxDistance = current
    return maxDistance

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


        
        #X = np.array(coordinates).reshape((1,-1),(1,-1))
        #X= scalar.transform(X)
        #print(X)
          
        #run kmeans with two clusters for starting point
        kmeans = KMeans(n_clusters=2, random_state=0).fit(X)
        maxDistance = getMaxDistance(kmeans)
        clusters = 2
        
        #run kmeans while acceptableDistance is not fulfilled
        while(maxDistance > acceptableDistance):
            clusters+=1
            kmeans = KMeans(n_clusters=clusters, random_state=0).fit(X)
            maxDistance = getMaxDistance(kmeans)
        print("we're done! max distance is: " + str(maxDistance) + " at " + str(clusters) + " clusters!")
    
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('dbe', 'https://data.boston.gov/export/245/954/')
        doc.add_namespace('dbg', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') 
        doc.add_namespace('svm','https://data.somervillema.gov/resource/')
        
        #define the agent
        this_script = doc.agent('alg:aquan_erj826#getCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource
        resource = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of taking in the resource
        get_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crimes, this_script)
        doc.usage(get_crimes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$top=1000&$format=json'
                  }
                  )
        
        #define the writeout 
        crimes = doc.entity('dat:aquan_erj826#crimes', {prov.model.PROV_LABEL:'Crimes List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get_crimes, endTime)
        doc.wasDerivedFrom(crimes, resource, get_crimes, get_crimes, get_crimes)

        repo.logout()
                  
        return doc





# FindKMeans.execute(False)

## eof
