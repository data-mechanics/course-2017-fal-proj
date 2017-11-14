from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl
import random
from math import radians, sin, cos, atan2, sqrt
import scipy.stats as ss
from sklearn.cluster import k_means
from geopy.distance import vincenty as Distance
from shapely.geometry import shape, Point, Polygon
import numpy as np
import sys

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]


class setOptimalHealthMarkets(dml.Algorithm):
    contributor = 'biel_otis'
    reads = ['biel_otis.ObesityData', 'biel_otis.BostonZoning']
    writes = ['biel_otis.OptimalHealthMarkets']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')

        obesityValues = list(repo['biel_otis.ObesityData'].find())
        if (trial==True):
            obesityValues = obesityValues
            print(obesityValues)
            sys.stdout.flush()        
            exit()
        else:
            print("WERE NOT IN TRIAL")
            sys.stdout.flush()
        mapValues = list(repo['biel_otis.BostonZoning'].find())
        obesityValues = [x for x in obesityValues if x['cityname'] == 'Boston']
        obesityLoc = project(obesityValues, lambda x: (float(x['geolocation']['latitude']), float(x['geolocation']['longitude'])))
        avg_dist = 9999999
        num_means = 1
        dist_sum = 0
        dists = []
        lats = [x for (x,y) in obesityLoc]
        longs = [y for (x,y) in obesityLoc]
        means = []
        while (avg_dist >= 0.5):
            dist_sum = 0
            means = k_means(obesityLoc, n_clusters=num_means)[0]
            pds = [(p, Distance(m, p).miles) for (m,p) in product(means, obesityLoc)]
            pd = aggregate(pds, min)
            for x in pd:
                dist_sum += x[1]
            avg_dist = dist_sum / len(pd)
            print(avg_dist)
            num_means += 1             

        correctedMeans = []
        means = means.tolist()
        inputs = {}
        inputs['means'] = means
        # adjustedMeans = [Point(m) for m in means]
        # flag = False
        # countVal = 0
        # countInval = 0
        # for f in mapValues[0]:
        #         #polygon = shape(feature[f])
        #     if (f == '_id'):
        #         continue
        #     else:
        #         poly = shape(mapValues[0][f])
        #         print(f)
        #         print(mapValues[0][f])
        #         exit()
        #         for mean in adjustedMeans:
        #             if (poly.contains(mean)):
        #                 countVal += 1
        #                 flag = True
        #             else:
        #                 countInval += 1
        # print("num valid ", countVal)
        # print("num inval ", countInval)
        # exit()
        repo.dropCollection("OptimalHealthMarkets")
        repo.createCollection("OptimalHealthMarkets")
        repo['biel_otis.OptimalHealthMarkets'].insert_many([inputs])
        repo['biel_otis.OptimalHealthMarkets'].metadata({'complete':True})
        print(repo['biel_otis.OptimalHealthMarkets'].metadata())
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
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:biel_otis#setOptimalHealthMarkets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        obesity_resource = doc.entity('dat:biel_otis#ObesityData', {prov.model.PROV_LABEL:'Obesity Data from City of Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        zoning_resource = doc.entity('dat:biel_otis#BostonZoning', {prov.model.PROV_LABEL:'Dataset containing geojson of the Boston Districts', prov.model.PROV_TYPE:'ont:DataSet'})
        output_resource = doc.entity('dat:biel_otis#OptimalHealthMarkets', {prov.model.PROV_LABEL: 'Dataset containing the optimal placements of health food markets based on locations of obese persons.', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    
        
        #Associations
        doc.wasAssociatedWith(this_run, this_script)
     
        #Usages
        doc.usage(this_run, obesity_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, zoning_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        #Generated
        doc.wasGeneratedBy(output_resource, this_run, endTime)


        #Attributions
        doc.wasAttributedTo(output_resource, this_script)

        #Derivations
        doc.wasDerivedFrom(output_resource, obesity_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(output_resource, zoning_resource, this_run, this_run, this_run)

        repo.logout()
        
        return doc

## eof
