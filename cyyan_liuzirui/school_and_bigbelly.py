import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import radians, sqrt, sin, cos, atan2
from geopy.distance import vincenty #need to install geopy first
from helper import *
import itertools


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

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

def geodistance(la1, lon1, la2, lon2):
        la1 = radians(la1)
        lon1 = radians(lon1)
        la2 = radians(la2)
        lon2 = radians(lon2)

        dlon = lon1 - lon2

        EARTH_R = 6372.8

        y = sqrt(
            (cos(la2) * sin(dlon)) ** 2
            + (cos(la1) * sin(la2) - sin(la1) * cos(la2) * cos(dlon)) ** 2
            )
        x = sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(dlon)
        c = atan2(y, x)
        return EARTH_R * c

class school_and_bigbelly(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = ['cyyan_liuzirui.school', 'cyyan_liuzirui.big_belly']
    writes = ['cyyan_liuzirui.school_and_bigbelly']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

            # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui', 'cyyan_liuzirui')

        # Get the school coordinates
        # Collect school coordinates with form [(school_name, latitude, longtitude)]
        school = repo['cyyan_liuzirui.school'].find()
        school_coordinates = []
        for i in school[1:]:
            if 'coordinates' in i:
                coordinate = i['coordinates']
                temp = coordinate.strip().split(",")
                la = temp[0]
                lon = temp[1]
                school_coordinates += [(i['name'], float(la), float(lon))]

        # Get the bigbelly coordinates
        # Collect big belly coordinates with form [(big belly name, latitude, longtitude)]
        big_belly = repo['cyyan_liuzirui.big_belly'].find()
        big_belly_coordinates = []
        for q in big_belly[1:]:
            if 'Location' in q:
                coordinate = q['Location']
                temp = coordinate.strip().split(",")
                #print("ddddd", temp)
                if temp == [''] or temp == []:
                    continue
                else:
                    coordinate = q['Location']
                    temp = coordinate.strip().split(",")
                    la = temp[0][1:]
                    lon = temp[-1][:-1]
                    big_belly_coordinates += [(q['description'], float(la), float(lon))]

        # calculate the distance between school and big belly
        # if the distance is less than three mile, insert such locations into dictionary
        dis = {}
        for i in school_coordinates:
            for j in big_belly_coordinates:
                distance = geodistance(i[1],i[2],j[1],j[2])
                if distance < 3.0:
                    dis[i[0] + ' to ' + j[0]] = distance

        # convert dictionary into a list of list
        results = [{'name':key, 'value':dis[key]} for key in dis]

        # drop collection school_and_bigbelly
        repo.drop_collection('school_and_bigbelly')
        # create collection school_and_bigbelly
        repo.create_collection('school_and_bigbelly')
        
        # insert selected data into mongodb
        repo['cyyan_liuzirui.school_and_bigbelly'].insert_many(results)
        repo['cyyan_liuzirui.school_and_bigbelly'].metadata({'complete': True})

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
        repo.authenticate('cyyan_liuzirui', 'cyyan_liuzirui')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('hpa', 'https://data.boston.gov/')
        # Define entity to represent resources
        this_script = doc.agent('alg:cyyan_liuzirui#school_and_bigbelly', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        # https://data.cityofboston.gov/resource/492y-i77g.json
        resource_school = doc.entity('dat:cyyan_liuzirui#school', {prov.model.PROV_LABEL:'school', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_bigbelly = doc.entity('dat:cyyan_liuzirui#big_belly', {prov.model.PROV_LABEL:'bigbelly', prov.model.PROV_TYPE:'ont:DataSet'})
        #https://data.boston.gov/export/15e/7fa/15e7fa44-b9a8-42da-82e1-304e43460095.json
        get_schoolLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # associate the activity with the script
        doc.wasAssociatedWith(this_script, get_schoolLocation)

        #indicate that an activity used the entity
        doc.usage(resource_school, get_schoolLocation, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(resource_bigbelly, get_schoolLocation, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )


        schoolLocation = doc.entity('dat:cyyan_liuzirui#schoolLocation', {prov.model.PROV_LABEL:'School', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schoolLocation, this_script)
        doc.wasGeneratedBy(schoolLocation, get_schoolLocation, endTime)
        doc.wasDerivedFrom(schoolLocation, resource_school, get_schoolLocation, get_schoolLocation, get_schoolLocation)

        bigbellyLocation = doc.entity('dat:cyyan_liuzirui#bigbellyLocation', {prov.model.PROV_LABEL:'Bigbelly', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bigbellyLocation, this_script)
        doc.wasGeneratedBy(bigbellyLocation, get_schoolLocation, endTime)
        doc.wasDerivedFrom(bigbellyLocation, resource_bigbelly, get_schoolLocation, get_schoolLocation, get_schoolLocation)

        repo.logout()

        return doc


# school_and_bigbelly.execute()
# doc = school_and_bigbelly.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof