from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl
import random
from shapely.geometry import shape, Point
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

class setObesityPropertyCorrelation(dml.Algorithm):
    print('setObesityPropertyCorrelation')
    contributor = 'biel_otis'
    reads = ['biel_otis.ObesityData', 'biel_otis.PropertyValues', 'biel_otis.BostonZoning']
    writes = ['biel_otis.ObesityPropertyCorrelation']

    @staticmethod
    def execute(trial = False):        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        obesityValues = list(repo['biel_otis.ObesityData'].find({"cityname": "Boston"}))
        propertyValues = list(repo['biel_otis.PropertyValues'].find())
        mapValues = list(repo['biel_otis.BostonZoning'].find())


        if (trial==True):
            obesityValues = obesityValues[0:100]
            propertyValues = propertyValues[0:100]

        propLoc = project(propertyValues, lambda x: (tuple(x['Location'].replace("(", "").replace(")", "").replace("|",",").split(",")), x['AV_TOTAL']))
        obesityLoc = project(obesityValues, lambda x: (float(x['geolocation']['latitude']), float(x['geolocation']['longitude'])))
        distances = [((float(x[0][0]),float(x[0][1])),(float(x[1]), 1)) for x in propLoc for y in obesityLoc if calculateDist((float(x[0][0]),float(x[0][1])),y) < 0.3 and x[1] != '0' and x[0][0] != '']
        
        for f in mapValues[0]:
            if (f == '_id'):
                continue
            else:
                if (f == "South Boston" or f == "South Boston Neighborhood"):
                    mapValues[0][f]['coordinates'][0][0] = [(x,y) for (y,x) in mapValues[0][f]['coordinates'][0][0]]
                else:
                    mapValues[0][f]['coordinates'][0] = [(x,y) for (y,x) in mapValues[0][f]['coordinates'][0]]


        neighborDict = {}
        for tup in distances:
            p = Point(tup[0])
            for f in mapValues[0]:
                if (f == '_id'):
                    continue
                else:
                    poly = shape(mapValues[0][f])
                    if (poly.contains(p)):
                        if (f in neighborDict):
                            neighborDict[f].append(tup[1])
                        else:
                            neighborDict[f] = [tup[1]]
        
        inputs = {}
        for hood in neighborDict:
            obeseCount = aggregate(neighborDict[hood], sum)
            bucketDict = {}
            bucket = 100000
            while (bucket < 5000000):
                for tup in obeseCount:
                    if (tup[0] <= bucket and tup[0] >= (bucket - 100000)):
                        if (bucket in bucketDict):
                            bucketDict[bucket] += tup[1]
                        else:
                            bucketDict[bucket] = tup[1]

                bucket+= 100000

            bucketList = list(bucketDict.items())
            x = [x for (x,y) in bucketList]
            y = [y for (x,y) in bucketList]

            correlation = ss.pearsonr(x, y)
            inputs[hood] = correlation


        allBoston = [tup[1] for tup in distances]
        allCount = aggregate(allBoston, sum)
        bucketDict = {}
        bucket = 100000
        while (bucket < 5000000):
            for tup in allCount:
                if (tup[0] <= bucket and tup[0] >= (bucket - 100000)):
                    if (bucket in bucketDict):
                        bucketDict[bucket] += tup[1]
                    else:
                        bucketDict[bucket] = tup[1]
            bucket += 100000
        
        bucketList = list(bucketDict.items())
        x = [x for (x,y) in bucketList]
        y = [y for (x,y) in bucketList]

        correlation = ss.pearsonr(x,y)
        inputs['All Boston'] = correlation
        #Now we have all of the obesity to property mappings within .3km of the eachother.
        #Now we must aggregate based on property location and the property value.
        #Then correlation coefficient.
    
        #



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
        zoning_resource = doc.entity('dat:biel_otis#BostonZoning', {prov.model.PROV_LABEL: 'Dataset containing geojson of shapefiles for neighborhoods in Boston.', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    
        
        #Associations
        doc.wasAssociatedWith(this_run, this_script)
     
        #Usages
        doc.usage(this_run, obesity_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, property_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, zoning_resource, starTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        #Generated
        doc.wasGeneratedBy(correlation_resource, this_run, endTime)


        #Attributions
        doc.wasAttributedTo(correlation_resource, this_script)

        #Derivations
        doc.wasDerivedFrom(correlation_resource, obesity_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(correlation_resource, property_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(correlation_resource, zoning_resource, this_run, this_run, this_run)

        repo.logout()
        
        return doc

setObesityPropertyCorrelation.execute()
## eof
