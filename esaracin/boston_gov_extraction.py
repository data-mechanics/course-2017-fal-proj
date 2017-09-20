import urllib.request
import pandas as pd
import json
import dml
import prov.model
import datetime
import uuid
import sys

class boston_gov_extraction(dml.Algorithm):
    contributor = 'esaracin'
    reads = []
    writes = ['esaracin.crime_incidents', 'esaracin.gun_data']

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
        url = 'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/crime.csv'
        dataset = pd.read_csv(url)
        json_set = dataset.to_json(orient='records')
        r = json.loads(json_set)


        # Add a collection to store our data, and store it.
        repo.dropCollection("crime_incidents")
        repo.createCollection("crime_incidents")
        repo['esaracin.crime_incidents'].insert_many(r)
        repo['esaracin.crime_incidents'].metadata({'complete':True})

        print('half way there!')


        # Do the same as above for our gun dataset
        url = 'https://data.boston.gov/dataset/3937b427-6aa4-4515-b30d-c76771313feb/resource/474f5374-15fe-4768-b31c-18b819cfa145/download/boston-police-department-firearms-recovery-counts.csv'
        dataset = pd.read_csv(url)
        json_set = dataset.to_json(orient='records')
        r = json.loads(json_set)

        repo.dropCollection("gun_data")
        repo.createCollection("gun_data")
        repo['esaracin.gun_data'].insert_many(r)
        repo['esaracin.gun_data'].metadata({'cmplete':True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 

boston_gov_extraction.execute()
