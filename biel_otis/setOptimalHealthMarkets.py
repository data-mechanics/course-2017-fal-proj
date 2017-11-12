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

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)


def scale(p, c):
    (x,y) = p
    return (x/c, y/c)


def calculateDist(d1, d2):
    R = 6373.0

    lat1 = radians(d1[0])
    lon1 = radians(d1[1])
    lat2 = radians(d2[0])
    lon2 = radians(d2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance 

def compTuples(t1, t2):
    if(t1 == []):
        return 100000000000000
    comp = [abs(x[0] - y[0]) + abs(x[1] - y[1]) for x in t1 for y in t2]
    return sum(comp)


def kmeans(means, points):
    old_compVal = 0
    new_compVal = 1
    mp = []
    old = []
    while (old_compVal != new_compVal):
        print('iterating')
        old_compVal = compTuples(old, means)
        old = means
        mpd = [(m, p, calculateDist(m, p)) for (m,p) in product(means, points)]
        pds = [(p, calculateDist(m,p)) for (m, p, d) in mpd]
        pd = aggregate(pds, min)
        mp = [(m, p) for ((m,p,d), (p2,d2)) in product(mpd, pd) if p==p2 and d==d2]
        mt = aggregate(mp, plus)
        m1 = [(m, 1) for ((m,p,d), (p2, d2)) in product(mpd, pd) if p==p2 and d==d2]
        mc = aggregate(m1, sum)

        means = [scale(t, c) for ((m,t), (m2,c)) in product(mt, mc) if m == m2]
        new_compVal = compTuples(old, means)

    return (means, mp)

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
        mapValues = list(repo['biel_otis.BostonZoning'].find())

        obesityLoc = project(obesityValues, lambda x: (float(x['geolocation']['latitude']), float(x['geolocation']['longitude'])))
        avg_dist = 9999999
        num_means = 1
        dist_sum = 0
        dists = []
        lats = [x for (x,y) in obesityLoc]
        longs = [y for (x,y) in obesityLoc]
        while (avg_dist >= 1.6):
            means = [(random.uniform(min(lats), max(lats)), random.uniform(min(longs), max(longs))) for x in range(num_means)]
            means, dists = kmeans(means, obesityLoc)
            for p in dists:
                dist_sum += calculateDist(p[0], p[1])
            avg_dist = dist_sum / len(dists)
            print(avg_dist)
            num_means += 1             

        exit()

        repo.dropCollection("ObesityPropertyCorrelation")
        repo.createCollection("ObesityPropertyCorrelation")
        repo['biel_otis.ObesityPropertyCorrelation'].insert_many([inputs])
        repo['biel_otis.ObesityPropertyCorrelation'].metadata({'complete':True})
        print(repo['biel_otis.ObesityPropertyCorrelation'].metadata())
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

        this_script = doc.agent('alg:biel_otis#setObesityPropertyCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        obesity_resource = doc.entity('dat:biel_otis#ObesityData', {prov.model.PROV_LABEL:'Obesity Data from City of Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        property_resource = doc.entity('dat:biel_otis#PropertyValues', {prov.model.PROV_LABEL:'Dataset containing property values & locations of properties', prov.model.PROV_TYPE:'ont:DataSet'})
        correlation_resource = doc.entity('dat:biel_otis#ObesityPropertyCorrelation', {prov.model.PROV_LABEL: 'Dataset containing one entry: the correlation coefficient between number of obese people within proximity to a property, and that properties value', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    
        
        #Associations
        doc.wasAssociatedWith(this_run, this_script)
     
        #Usages
        doc.usage(this_run, obesity_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, property_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        #Generated
        doc.wasGeneratedBy(correlation_resource, this_run, endTime)


        #Attributions
        doc.wasAttributedTo(correlation_resource, this_script)

        #Derivations
        doc.wasDerivedFrom(correlation_resource, obesity_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(correlation_resource, property_resource, this_run, this_run, this_run)

        repo.logout()
        
        return doc

setOptimalHealthMarkets.execute()

## eof
