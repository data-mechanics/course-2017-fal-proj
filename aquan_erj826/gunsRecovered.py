"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

gunsRecovered.py

Transformation:
A projection from aquan_erj826.firearms to give a 
dataset in the form: {Date: Total Firearms Collected}
"""

import json
import dml
import prov.model
import datetime
import uuid

class gunsRecovered(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = ['aquan_erj826.firearms']
    writes = ['aquan_erj826.gunsRecovered']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aquan_erj826', 'aquan_erj826')

        collection = repo.aquan_erj826.firearms

        repo.dropCollection("gunsRecovered")
        repo.createCollection("gunsRecovered")

        for point in collection.find():
            total = int(point['buybackgunsrecovered']) + \
                    int(point['crimegunsrecovered']) + \
                    int(point['gunssurrenderedsafeguarded'])
            date = point['collectiondate'][:10]
            repo['aquan_erj826.gunsRecovered'].insert([{'DATE':date, 'TOTAL_GUNS_COLLECTED':total}], check_keys=False)

        repo['aquan_erj826.gunsRecovered'].metadata({'complete':True})
        print(repo['aquan_erj826.gunsRecovered'].metadata())


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
        this_script = doc.agent('alg:aquan_erj826#gunsRecovered', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the firearms.json that we are using
        resource = doc.entity('dat:aquan_erj826#firearms', {'prov:label':'firearmsCollectionMongoDB', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the calculation that we did
        get_summation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_summation, this_script)
        doc.usage(get_summation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation'}
                  )

        #define write entity
        gunsRecoveredNums = doc.entity('dat:aquan_erj826#gunsRecoveredNums', {prov.model.PROV_LABEL:'Guns Recovered List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(gunsRecoveredNums, this_script)
        doc.wasGeneratedBy(gunsRecoveredNums, get_summation, endTime)
        doc.wasDerivedFrom(gunsRecoveredNums, resource, get_summation, get_summation,get_summation)

        repo.logout()
                  
        return doc

#gunsRecovered.execute()
#doc = gunsRecovered.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
