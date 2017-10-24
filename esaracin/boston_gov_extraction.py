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



        # Do the same as above for our gun dataset
        url = 'https://data.boston.gov/dataset/3937b427-6aa4-4515-b30d-c76771313feb/resource/474f5374-15fe-4768-b31c-18b819cfa145/download/boston-police-department-firearms-recovery-counts.csv'
        dataset = pd.read_csv(url)
        json_set = dataset.to_json(orient='records')
        r = json.loads(json_set)

        repo.dropCollection("gun_data")
        repo.createCollection("gun_data")
        repo['esaracin.gun_data'].insert_many(r)
        repo['esaracin.gun_data'].metadata({'complete':True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''Creates the provenance document describing the collection of data
        occuring within this script.'''
        
        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Add useful namespaces for this prov doc
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics/io/ontology/')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('gov1','https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/')
        doc.add_namespace('gov2', 'https://data.boston.gov/dataset/3937b427-6aa4-4515-b30d-c76771313feb/resource/474f5374-15fe-4768-b31c-18b819cfa145/download/') # Namespace specific to this script

        # Add this script as a provenance agent to our document
        this_script = doc.agent('alg:esaracin#boston_gov_extraction', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('gov1:crime', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})
        resource_guns = doc.entity('gov2:boston-police-department-firearms-recovery-counts',
                   {'prov:label':'311, Service Requests',
                    prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_guns = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_guns, this_script)
        doc.usage(get_crime, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_guns, resource_guns, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})


        crimes = doc.entity('dat:esaracin#crime_incidents', {prov.model.PROV_LABEL:'Crimes Reported', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get_crime, endTime)
        doc.wasDerivedFrom(crimes, resource, get_crime, get_crime, get_crime)

        guns = doc.entity('dat:esaracin#gun_data', {prov.model.PROV_LABEL:'Guns Collected',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(guns, this_script)
        doc.wasGeneratedBy(guns, get_guns, endTime)
        doc.wasDerivedFrom(guns, resource_guns, get_guns, get_guns, get_guns)

        repo.logout()
        return doc

