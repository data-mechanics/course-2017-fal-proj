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
        doc.add_namespace('bos', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/') # Namespace specific to this script

        # Add this script as a provenance agent to our document
        this_script = doc.agent('alg:esaracin#boston_opendata_extraction', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_stations = doc.entity('bos:e5a0066d38ac4e2abbc7918197a4f6af_6', {'prov:label':'311, Service Requests',
                    prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})

        resource_districts = doc.entity('bos:9a3a8c427add450eaf45a470245680fc_5', {'prov:label':'311, Service Requests',
                    prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_districts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stations, this_script)
        doc.wasAssociatedWith(get_districts, this_script)
        doc.usage(get_stations, resource_stations, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_districts, resource_districts, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})


        stations = doc.entity('dat:esaracin#police_stations',
                              {prov.model.PROV_LABEL:'Police Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, resource_stations, get_stations,
                           get_stations, get_stations)

        districts = doc.entity('dat:esaracin#police_districts',
                               {prov.model.PROV_LABEL:'Police Districts',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(districts, this_script)
        doc.wasGeneratedBy(districts, get_districts, endTime)
        doc.wasDerivedFrom(districts, resource_districts, get_districts,
                           get_districts, get_districts)

        repo.logout()


        return doc

boston_opendata_extraction.execute()
boston_opendata_extraction.provenance()
