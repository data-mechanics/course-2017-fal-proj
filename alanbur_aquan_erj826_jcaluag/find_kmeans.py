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



#our algorithm
class FindKMeans():
    #the largest acceptable distance between two data 
    
    coordinates = []

    def __init__(self,distance):
        # convert miles input to lat/long degrees
        self.acceptableDistance = distance / 69 # 1° ≈ 69 miles

    #takes all the coordinates and the kmeans cluster and returns the highest distance between
    #any point and its respective centroid    
    def getMaxDistance(self,kmeans, coordinates):
        maxDistance = 0
        for i in range(len(coordinates)):

            clusterCenter = kmeans.predict([coordinates[i]])

            current = self.distance(kmeans.cluster_centers_[clusterCenter][0],coordinates[i])

            if(current > maxDistance):
                maxDistance = current
        return maxDistance

    def getAvgDistance(self,kmeans, coordinates):
        totalDistance = 0
        for i in range(len(coordinates)):
            clusterCenter = kmeans.predict([coordinates[i]])
            totalDistance += self.distance(kmeans.cluster_centers_[clusterCenter][0],coordinates[i])
        return totalDistance/len(coordinates)
            
    #returns the distance between two long/lat points
    def distance(self,p0, p1):
        return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)


    def execute(self,toggle,trial = False):
        #param: toggle - true for average or false for max distance
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()
        #print('Finding optimal number of means')
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
                datapoint = [entry['latitude'],entry['longitude']]
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
           # print('Running in trial mode')
            coordinates=TrialSample
           # print(coordinates)

        X = np.array(coordinates)

        #True means find with averages, False means find with maxes
        avgOrMaxDistToggle = toggle

        #run kmeans with two clusters for starting point
        kmeans = KMeans(n_clusters=2, random_state=0).fit(X)
        
        if(avgOrMaxDistToggle):
            metric = self.getAvgDistance(kmeans,coordinates)
        else:
            print('test')
            metric = self.getMaxDistance(kmeans,coordinates)
        clusters = 2

        
        #run kmeans while acceptableDistance is not fulfilled
        while(metric > self.acceptableDistance):
            #print("the metric is: " + str(metric))
            clusters+=1
            kmeans = KMeans(n_clusters=clusters, random_state=0).fit(X) 
            if(avgOrMaxDistToggle):
                metric = self.getAvgDistance(kmeans,coordinates)
            else:

                metric = self.getMaxDistance(kmeans,coordinates)
      #  print("we're done! the " +  str({True: "avg", False: "max"} [avgOrMaxDistToggle])+ " distance is: " + str(metric) + " at " + str(clusters) + " clusters!")
    
        #plug into the centroids into dictionary for returning
        n=[]
        centroids = kmeans.cluster_centers_.tolist()
        for each in centroids:
            n += [each]

        # return centroids
      #  print(repo['alanbur_aquan_erj826_jcaluag.kMeansNY'].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

        return n






# FindKMeans.execute(False)

## eof
