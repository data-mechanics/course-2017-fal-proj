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
    contributor = 'biel_otis'
    reads = ['biel_otis.ObesityData', 'biel_otis.PropertyValues']
    writes = ['biel_otis.ObesityPropertyCorrelation']

    @staticmethod
    def execute(trial = False):        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')

        obesityValues = list(repo['biel_otis.ObesityData'].find())
        propertyValues = list(repo['biel_otis.PropertyValues'].find())

        #print(propertyValues)
        #print(obesityValues)
        propLoc = project(propertyValues, lambda x: (tuple(x['location'].replace("(", "").replace(")", "").split(",")), x['av_total']))
        obesityLoc = project(obesityValues, lambda x: (float(x['geolocation']['latitude']), float(x['geolocation']['longitude'])))
        distances = [(x, y) for x in propLoc for y in obesityLoc if calculateDist((float(x[0][0]),float(x[0][1])),y) < 0.3]
        print(distances[0])
        #Now we have all of the obesity to property mappings within .3km of the eachother.
        #Now we must aggregate based on property location and the property value.
        #Then correlation coefficient.
    
        #
        exit()



        repo.dropCollection("ObesityPropertyCorrelation")
        repo.createCollection("ObesityPropertyCorrelation")
        repo['biel_otis.ObesityPropertyCorrelation'].insert_many(inputs)
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

        this_script = doc.agent('alg:biel_otis#setObesityMarkets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        obesity = doc.entity('dat:biel_otis#obesity', {prov.model.PROV_LABEL:'Obesity Data from City of Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        obesityMarkets = doc.entity('dat:biel_otis#obesity_market_locations', {prov.model.PROV_LABEL:'Dataset containing the optimal locations for healty food markets', prov.model.PROV_TYPE:'ont:DataSet'})


        get_obesityMarkets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_obesityMarkets, this_script)
        
        doc.usage(get_obesityMarkets, obesity, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Transformation'})

        doc.wasAttributedTo(obesityMarkets, this_script)
        doc.wasGeneratedBy(obesityMarkets, obesityMarkets, endTime)
        doc.wasDerivedFrom(obesity, get_obesityMarkets, get_obesityMarkets, get_obesityMarkets, get_obesityMarkets, get_obesityMarkets)
        repo.logout()
        
        return doc

setObesityPropertyCorrelation.execute()

## eof
