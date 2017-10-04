"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

crimeDispatch.py

Transformation:
Selection and projection on repo.aquan_erj826.Counts911, 
and an aggregation with repo.aquan_erj826.crimes to find 
the number of 911 calls that did not correspond to crime 
incident reports in 2014
"""

import json
import dml
import prov.model
import datetime
import uuid
from bson.objectid import ObjectId

class crimeDispatch(dml.Algorithm):
    contributor = 'erj826'
    reads = []
    writes = ['aquan_erj826.crimeDispatch']


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('erj826', 'erj826')
        

        dispatch = repo.aquan_erj826.Counts911
        crimeReports = repo.aquan_erj826.crimes

        repo.dropCollection("crimeDispatch")
        repo.createCollection("crimeDispatch")

        for call in dispatch.find():
            date = call['Date'].split('/')
            date = date[0] + '-' + date[1] + '-' + date[2]
            total = call['Total']
            if date[-4::] == '2014':
                repo['aquan_erj826.crimeDispatch'].insert([{date:total}], check_keys=True)

        crimeDispatch = repo.aquan_erj826.crimeDispatch

        dates = []
        for report in crimeReports.find():
            #Adding an arbitrary year to the date
            date = report['OCCURRED_ON_DATE'].split('T')[0][5:] + '-2014'
            dates.append(date)

        for point in crimeDispatch.find():
            date = list(point.keys())[1]
            total = point[date]

            if date in dates:
                new_total = int(total) - 1
                dates.remove(date)

                crimeDispatch.update_one({
                   '_id': point['_id']
                    },{
                      '$set': {
                       date: new_total
                      }
                    }, upsert=False)

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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

crimeDispatch.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
