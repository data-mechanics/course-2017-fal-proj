import urllib.request
import json
import dml, prov.model
import datetime, uuid
import geojson
# import csv

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

City of Boston Open Spaces (Like parks, etc)

Development notes:

csv alt link:
url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.csv'

"""

class retrieve_open_space(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.open_space']

    @staticmethod
    def execute(trial = False, log=False):
        '''Retrieves open spaces in Boston as geoJSON'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        # Do retrieving of data
        repo.dropCollection("open_space")
        repo.createCollection("open_space")    
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")        
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['bmroach.open_space'].insert_many( geoList )
        repo['bmroach.open_space'].metadata({'complete':True})  
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

        
                  
        return 





retrieve_open_space.execute()

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
