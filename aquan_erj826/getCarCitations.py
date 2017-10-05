"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

getCarCitations.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getCarCitations(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = []
    writes = ['aquan_erj826.carCitations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve motor vehicle citation information from Somerville.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('aquan_erj826', 'aquan_erj826')          

        url = 'https://data.somervillema.gov/resource/jpgd-3f23.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("carCitations")
        repo.createCollection("carCitations")
        repo['aquan_erj826.carCitations'].insert_many(r)
        repo['aquan_erj826.carCitations'].metadata({'complete':True})
        print(repo['aquan_erj826.carCitations'].metadata())

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
        repo.authenticate('erj826', 'erj826')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        #resources:
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('dbe', 'https://data.boston.gov/export/245/954/')
        doc.add_namespace('dbg', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') 
        doc.add_namespace('svm','https://data.somervillema.gov/resource/')

        #define the agent
        this_script = doc.agent('alg:aquan_erj826#getCarCitations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource (entity)
        resource = doc.entity('svm:jpgd-3f23.json', {'prov:label':'Car Violation Citations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of getting the resource 
        get_carCitations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_carCitations, this_script)
        doc.usage(get_carCitations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        
        #define the entity of writeout
        carCitations = doc.entity('dat:aquan_erj826#carCitations', {prov.model.PROV_LABEL:'Car Citations Set', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(carCitations, this_script)
        doc.wasGeneratedBy(carCitations, get_carCitations, endTime)
        doc.wasDerivedFrom(carCitations, resource, get_carCitations, get_carCitations, get_carCitations)

        repo.logout()
                  
        return doc

#getCarCitations.execute()
#doc = getCarCitations.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

