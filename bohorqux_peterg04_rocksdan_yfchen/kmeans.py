import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import random
import math
from scipy.cluster.vq import kmeans as km, kmeans2
# import geopy
# from geopy.geocoders import great_circle

# Credits
# We are taking this database from eileenli_yidingou from their previous project #1

class kmeans(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.Restaurants']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.kmeans']
        
    def plus(args):
        p = [0,0]
        for (x,y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)
    
    def dist(p, q):
        (x1,y1) = p
        (x2,y2) = q
        return math.sqrt((x1-x2)**2 + (y1-y2)**2)

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        restaurants = repo['bohorqux_peterg04_rocksdan_yfchen.Restaurants']
        
        # Get all the coordinates of each restaurant
        P = []
        for entry in restaurants.find():
            if entry["location"]["coordinates"][0] != 0 and entry["location"]["coordinates"][1] != 0:
                xcoord = entry["location"]["coordinates"][0]
                ycoord = entry["location"]["coordinates"][1]
                P.append((xcoord, ycoord))
                
#         # This part of the code is commented out -- it was used solely for the case of choosing K, for
#         # kmeans by implementing the elbow method below. A line chart has been put into the readme
#         sse = dict()
#         for k in range(1,10):
#             sse[k] = 0;
#             centroids, labels = kmeans2(P,k)
#             # for each cluster/mean
#             center_index = 0
#             for center in centroids:
#                 datapoint_index = 0
#                 mean = (center[0], center[1])
#                 # take the datapoints associated with that cluster/mean and add their sse
#                 # the formula is sse[k] += Math.pow(distance between two points, 2)
#                 for datapoint in labels:
#                     if datapoint == center_index:
#                         sse[k] += math.pow(kmeans.dist(P[datapoint_index], mean), 2)
#                     datapoint_index += 1
#                     
#                 center_index += 1
#         print(sse)    

        # After printing it out and looking at our graph, we see that the elbow test states
        # that we should have a total of 5 means/centroids      	
        centroids, labels = kmeans2(P,5)
        means = []
        
        for x in range(len(centroids)):
            means.append((centroids[x][0], centroids[x][1]))
         
        final_means = dict()
        final_means['center1'] = dict()
        final_means['center1']['centroid'] = means[0]
        final_means['center2'] = dict()
        final_means['center2']['centroid'] = means[1]
        final_means['center3'] = dict()
        final_means['center3']['centroid'] = means[2]
        final_means['center4'] = dict()
        final_means['center4']['centroid'] = means[3]
        final_means['center5'] = dict()
        final_means['center5']['centroid'] = means[4]
        
        # Now that we have the final means/centroids, we will use this to further along our goal and 
        # apply a radius to these centroids by making the radius equal to the distance of the centroid
        # to their farthest associated datapoint
        
        # make a list of radii pertaining to the longest distance for each centroid
        radii = []
        center_index = 0
        for (x,y) in means:
            largest_distance = 0
            C = (x,y)
            datapoint_index = 0
            for datapoint in labels:
                if datapoint == center_index:
#                     center_datapoint[center_index].append((P[datapoint_index][0], P[datapoint_index][1]))
                    temp = kmeans.dist(means[center_index], P[datapoint_index])
                    if temp > largest_distance:
                        largest_distance = temp
                datapoint_index += 1
            
            radii.append(largest_distance)
            center_index += 1    
                    
        # Now with all the appropiate distance radii stored in order for each center, append the radii
        # to the correct centers within the final_means dictionary
        final_means['center1']['radius'] = radii[0]
        final_means['center2']['radius'] = radii[1]
        final_means['center3']['radius'] = radii[2]
        final_means['center4']['radius'] = radii[3]
        final_means['center5']['radius'] = radii[4]
        # {'center1' : {'centroid': [0.12, 0.145], 'radius': 0.12151 }}
   		
        repo.dropCollection("kmeans")
        repo.createCollection("kmeans")
        repo['bohorqux_peterg04_rocksdan_yfchen.kmeans'].insert(final_means)
        repo['bohorqux_peterg04_rocksdan_yfchen.kmeans'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.kmeans'].metadata())

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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
#         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
#         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
#         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
#         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
# 
#         this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#getRestaurants', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#         resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#         get_Restaurants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#         doc.wasAssociatedWith(get_Restaurants, this_script)
#         doc.usage(get_Restaurants, resource, startTime, None,
#                   {prov.model.PROV_TYPE:'ont:Retrieval',
#                   'ont:Query':'?type=BostonLife+Restaurants&$select=type,latitude,longitude,OPEN_DT'
#                   }
#                   )
# 
#         Restaurants = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#Restaurants', {prov.model.PROV_LABEL:'BostonLife Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
#         doc.wasAttributedTo(Restaurants, this_script)
#         doc.wasGeneratedBy(Restaurants, getRestaurants, endTime)
#         doc.wasDerivedFrom(Restaurants, resource, getRestaurants, getRestaurants, getRestaurants)

        repo.logout()
                  
        return doc

kmeans.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof