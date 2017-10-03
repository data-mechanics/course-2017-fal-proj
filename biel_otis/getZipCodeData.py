from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


"""
Helper Functions courtesy of Andrei Lapets - CS591 BU
"""
from math import sin, cos, sqrt, atan2, radians
import math

# approximate radius of earth in km

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
    d1 = d1.replace("(", "").replace(")", "")
    d1 = d1.split(",")
    d1 = (float(d1[0]), float(d1[1]))

    d2 = d2.replace("(", "").replace(")", "")
    d2 = d2.split(",")
    d2 = (float(d2[0]), float(d2[1]))

    lat1 = radians(d1[0])
    lon1 = radians(d1[1])
    lat2 = radians(d2[0])
    lon2 = radians(d2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance <= 1


def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def compTuples(t1, t2):
    if(t1 == []):
        return 100000000000000
    comp = [abs(x[0] - y[0]) + abs(x[1] - y[1]) for x in t1 for y in t2]
    return sum(comp)


class getZipCodeData(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.ZipCodes']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'http://datamechanics.io/data/biel_otis/zipcodes.json'
        response = urlopen(url).read().decode("utf-8")
        
        r = json.loads(response)
        myDict = {}
        myDict['1'] = []
        myList = [myDict]
        for i in r:
            myList[0]['1'].append(i)
        
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("ZipCodes")
        repo.createCollection("ZipCodes")
        repo['biel_otis.ZipCodes'].insert_many(myList)
        repo['biel_otis.ZipCodes'].metadata({'complete':True})
        print(repo['biel_otis.ZipCodes'].metadata())

        """
        url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("found")
        repo.createCollection("found")
        repo['biel_otis.found'].insert_many(r)
        """
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
        doc.add_namespace('zip', 'http://datamechanics.io/biel_otis/') # Dataset containing zipcode information from ZipCodes in Boston

        this_script = doc.agent('alg:biel_otis#getZipCodeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('zip:zipcodes', {'prov:label':'Dataset containing zipcode information from ZipCodes in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_zips = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zips, this_script)
        
        doc.usage(get_zips, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        zips = doc.entity('dat:biel_otis#zipcodes', {prov.model.PROV_LABEL:'Dataset containing zipcode information from ZipCodes in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zips, this_script)
        doc.wasGeneratedBy(zips, get_zips, endTime)
        doc.wasDerivedFrom(zips, resource, get_zips, get_zips, get_zips)
        repo.logout()
        
        return doc

getZipCodeData.execute()
doc = getZipCodeData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
