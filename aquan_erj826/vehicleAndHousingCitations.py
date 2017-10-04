"""
Eric Jacobson
erj826@bu.edu

Andrew Quan
aquan@bu.edu

CS591
Project 1

3 October 2017

vehicleAndHousingCitations.py

Transformation:
An aggregation and projection from aquan_erj826.carCitations and 
aquan_erj826.housingViolations to give a dataset in the form: 
{Date, Time, Type, Charge}
"""

import json
import dml
import prov.model
import datetime
import uuid

class vehicleAndHousingCitations(dml.Algorithm):
    contributor = 'erj826'
    reads = []
    writes = ['aquan_erj826.vehicleAndHousingCitations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('erj826', 'erj826')

        vehicles = repo.aquan_erj826.carCitations
        houses = repo.aquan_erj826.housingViolations


        repo.dropCollection("vehicleAndHousingCitations")
        repo.createCollection("vehicleAndHousingCitations")

        for car in vehicles.find():
            try:
                total_date = car['dtissued'].split('T')
            except:
                continue
            date = total_date[0]
            time = total_date[1]
            try:
                charge = car['chgdesc']
            except:
                continue
            kind = 'MOTOR VEHICLE'
            repo['aquan_erj826.vehicleAndHousingCitations'].insert([{'DATE':date, 'TIME':time,'TYPE':kind, 'CHARGE':charge}], check_keys=False)

        for house in houses.find():
            total_date = house['cited'].split('T')
            date = total_date[0]
            time = total_date[1]
            charge = house['code_description']
            kind = 'HOUSING'
            repo['aquan_erj826.vehicleAndHousingCitations'].insert([{'DATE':date, 'TIME':time,'TYPE':kind, 'CHARGE':charge}], check_keys=False)


        for item in repo['aquan_erj826.vehicleAndHousingCitations'].find():
            print(item)

        repo['aquan_erj826.vehicleAndHousingCitations'].metadata({'complete':True})
        print(repo['aquan_erj826.vehicleAndHousingCitations'].metadata())


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

vehicleAndHousingCitations.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
