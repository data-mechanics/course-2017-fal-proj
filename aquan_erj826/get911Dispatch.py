"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

get911Dispatch.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get911Dispatch(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = []
    writes = ['aquan_erj826.Counts911']

    @staticmethod
    def execute(trial = False):
        '''Retrieve 911 DAILY DISPATCH COUNT BY AGENCY data set from analyze Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('aquan_erj826', 'aquan_erj826')          

        url = 'https://data.boston.gov/export/245/954/2459542e-7026-48e2-9128-ca29dd3bebf8.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Counts911")
        repo.createCollection("Counts911")
        repo['aquan_erj826.Counts911'].insert_many(r)
        repo['aquan_erj826.Counts911'].metadata({'complete':True})
        print(repo['aquan_erj826.Counts911'].metadata())

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
        this_script = doc.agent('alg:aquan_erj826#get911Counts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource (entity)
        resource = doc.entity('dbe:2459542e-7026-48e2-9128-ca29dd3bebf8.json', {'prov:label':'911 Counts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of grabbing the 911 counts
        get_counts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_counts, this_script)
        doc.usage(get_counts, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        #writeout entity def
        dispatchData = doc.entity('dat:aquan_erj826#Counts911', {prov.model.PROV_LABEL:'911 dataset', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(dispatchData, this_script)
        doc.wasGeneratedBy(dispatchData, get_counts, endTime)
        doc.wasDerivedFrom(dispatchData, resource, get_counts, get_counts, get_counts)

        repo.logout()
                  
        return doc

#get911Dispatch.execute()
#doc = get911Dispatch.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

