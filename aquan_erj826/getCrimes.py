"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

getCrimes.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getCrimes(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = []
    writes = ['aquan_erj826.crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('erj826', 'erj826')          

        url = 'https://data.boston.gov/datastore/odata3.0/12cb3883-56f5-47de-afa5-3b1cf61b257b?$top=1000&$format=json'
        response = requests.get(url).text
        
        r = json.loads(response)

        repo.dropCollection("crimes")
        repo.createCollection("crimes")
        repo['aquan_erj826.crimes'].insert(r, check_keys=False)
        repo['aquan_erj826.crimes'].metadata({'complete':True})
        print(repo['aquan_erj826.crimes'].metadata())

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
        doc.add_namespace('dbg', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') 
        doc.add_namespace('svm','https://data.somervillema.gov/resource/')
        
        #define the agent
        this_script = doc.agent('alg:aquan_erj826#getCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource
        resource = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of taking in the resource
        get_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crimes, this_script)
        doc.usage(get_crimes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$top=1000&$format=json'
                  }
                  )
        
        #define the writeout 
        crimes = doc.entity('dat:aquan_erj826#crimes', {prov.model.PROV_LABEL:'Crimes List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get_crimes, endTime)
        doc.wasDerivedFrom(crimes, resource, get_crimes, get_crimes, get_crimes)

        repo.logout()
                  
        return doc

#getCrimes.execute()
#doc = getCrimes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

