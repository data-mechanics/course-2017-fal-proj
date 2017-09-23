import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid
import sys

class boston_opendata_extraction(dml.Algorithm):
    contributor = 'esaracin'
    reads = []
    writes = ['esaracin.police_stations', 'esaracin.police_districts']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Where we have to request and parse our dataset.
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.csv'
        dataset = pd.read_csv(url)
        json_set = dataset.to_json(orient='records')
        r = json.loads(json_set)

        # Add a collection to store our data, and store it.
        repo.dropCollection("police_stations")
        repo.createCollection("police_stations")
        repo['esaracin.police_stations'].insert_many(r)
        repo['esaracin.police_stations'].metadata({'complete':True})
        print(repo['esaracin.police_stations'].metadata())



        # Do the same as above for our other dataset.
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/9a3a8c427add450eaf45a470245680fc_5.csv'
        dataset = pd.read_csv(url)
        json_set = dataset.to_json(orient='records')
        r = json.loads(json_set)

        # Add a collection to store our data, and store it.
        repo.dropCollection("police_districts")
        repo.createCollection("police_districts")
        repo['esaracin.police_districts'].insert_many(r)
        repo['esaracin.police_districts'].metadata({'complete':True})
        print(repo['esaracin.police_districts'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 

boston_opendata_extraction.execute()
