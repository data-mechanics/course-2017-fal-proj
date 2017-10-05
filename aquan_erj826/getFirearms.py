"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

getFirearms.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getFirearms(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = []
    writes = ['aquan_erj826.firearms']

    @staticmethod
    def execute(trial = False):
        '''Retrieve firearms recovery data set from analyze Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('aquan_erj826', 'aquan_erj826')          

        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("firearms")
        repo.createCollection("firearms")
        repo['aquan_erj826.firearms'].insert_many(r)
        repo['aquan_erj826.firearms'].metadata({'complete':True})
        print(repo['aquan_erj826.firearms'].metadata())

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
        
        #declare agent, input entity, and activity
        this_script = doc.agent('alg:aquan_erj826#getFirearms', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:ffz3-2uqv.json', {'prov:label':'City of Boston Firearms Records', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_gunList = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        #agent <-> activity
        doc.wasAssociatedWith(get_gunList, this_script)
        
        #activity <-> entity
        doc.usage(get_gunList, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?=entireSet'
                  }
                  )
        #output entity
        gunList = doc.entity('dat:aquan_erj826#firearms', {prov.model.PROV_LABEL:'Firearms Collection on MongoDB', prov.model.PROV_TYPE:'ont:DataSet'})
       
        #output made by agent
        doc.wasAttributedTo(gunList, this_script)
        
        #output entity made by activty
        doc.wasGeneratedBy(gunList, get_gunList, endTime)
        
        #output entity derived from itself and activity
        doc.wasDerivedFrom(gunList, resource, get_gunList, get_gunList, get_gunList)
        
        repo.logout()
                  
        return doc

#getFirearms.execute()
#doc = getFirearms.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

