import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid
import sys

class city_of_boston_extraction(dml.Algorithm):
    contributor = 'esaracin'
    reads = []
    writes = ['esaracin.boston_shootings']

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
        url = 'https://data.cityofboston.gov/api/views/w4k7-yvrq/rows.csv?accessType=DOWNLOAD'
        dataset = pd.read_csv(url)
        json_set = dataset.to_json(orient='records')
        r = json.loads(json_set)


        # Add a collection to store our data, and store it.
        repo.dropCollection("boston_shootings")
        repo.createCollection("boston_shootings")
        repo['esaracin.boston_shootings'].insert_many(r)
        repo['esaracin.boston_shootings'].metadata({'complete':True})


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 

city_of_boston_extraction.execute()
