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
        doc.add_namespace('cit', 'https://data.cityofboston.gov/api/views/w4k7-yvrq/') # Namespace specific to this script

        # Add this script as a provenance agent to our document
        this_script = doc.agent('alg:esaracin#city_of_boston_extraction', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_shootings = doc.entity('cit:rows', {'prov:label':'311, Service Requests',
                    prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})

        get_shootings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_shootings, this_script)
        doc.usage(get_shootings, resource_shootings, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?accessType=DOWNLOAD'
                  })

        shootings = doc.entity('dat:esaracin#boston_shootings',
                              {prov.model.PROV_LABEL:'Shootings in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(shootings, this_script)
        doc.wasGeneratedBy(shootings, get_shootings, endTime)
        doc.wasDerivedFrom(shootings, resource_shootings, get_shootings,
                           get_shootings, get_shootings)


        repo.logout()
        return doc

