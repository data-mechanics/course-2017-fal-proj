"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

getHousingViolations.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getHousingViolations(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = []
    writes = ['aquan_erj826.housingViolations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve housing code violations information from Cambridge.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('erj826', 'erj826')          

        url = 'https://data.cambridgema.gov/resource/bepf-husa.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("housingViolations")
        repo.createCollection("housingViolations")
        repo['aquan_erj826.housingViolations'].insert_many(r)
        repo['aquan_erj826.housingViolations'].metadata({'complete':True})
        print(repo['aquan_erj826.housingViolations'].metadata())

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

        #define the agent, this script
        this_script = doc.agent('alg:aquan_erj826#getHousingViolations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the resource we pulled from (entity)
        resource = doc.entity('cdp:bepf-husa.json', {'prov:label':'Housing Violation Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of grabbing the resource
        get_housingViolations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_housingViolations, this_script)
        doc.usage(get_housingViolations, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',}    
                  )
        
        #define writeout entity
        housingViolations = doc.entity('dat:aquan_erj826#housingViolations', {prov.model.PROV_LABEL:'housingViolationsListMade', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(housingViolations, this_script)
        doc.wasGeneratedBy(housingViolations, get_housingViolations, endTime)
        doc.wasDerivedFrom(housingViolations, resource, get_housingViolations, get_housingViolations, get_housingViolations)

        repo.logout()
                  
        return doc

#getHousingViolations.execute()
#doc = getHousingViolations.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

