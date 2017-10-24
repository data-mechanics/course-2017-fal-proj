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
    contributor = 'aquan_erj826'
    reads = ['aquan_erj826.carCitations', 'aquan_erj826.housingViolations']
    writes = ['aquan_erj826.vehicleAndHousingCitations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aquan_erj826', 'aquan_erj826')

        #The transformations will involve car citation and housing violation datasets
        vehicles = repo.aquan_erj826.carCitations
        houses = repo.aquan_erj826.housingViolations


        repo.dropCollection("vehicleAndHousingCitations")
        repo.createCollection("vehicleAndHousingCitations")

        for car in vehicles.find():
            try:
                #Get the date that the incident occured ([Date, Time])
                total_date = car['dtissued'].split('T')
            except:
                continue
            date = total_date[0]
            time = total_date[1]
            try:
                #Get the description of the motor vehicle violation
                charge = car['chgdesc']
            except:
                continue
            kind = 'MOTOR VEHICLE'
            #Put data into mongo
            repo['aquan_erj826.vehicleAndHousingCitations'].insert([{'DATE':date, 'TIME':time,'TYPE':kind, 'CHARGE':charge}], check_keys=False)


        for house in houses.find():
            #Get the date that the violation was reported ([Date,Time])
            total_date = house['cited'].split('T')
            date = total_date[0]
            time = total_date[1]
            charge = house['code_description']
            kind = 'HOUSING'
            #Put data into mongo
            repo['aquan_erj826.vehicleAndHousingCitations'].insert([{'DATE':date, 'TIME':time,'TYPE':kind, 'CHARGE':charge}], check_keys=False)


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
        this_script = doc.agent('alg:aquan_erj826#vehicleAndHousingCitations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the Parking Violations we are using (entity)
        carCitations = doc.entity('dat:aquan_erj826#carCitations', {'prov:label':'CarViolationsFromPriorRetrieval', prov.model.PROV_TYPE:'ont:DataSet'})
        
        #define the Housing Violations we are using(entity)
        housingViolations = doc.entity('dat:aquan_erj826#housingViolations', {'prov:label':'housingViolationsFromPriorRetrieval', prov.model.PROV_TYPE:'ont:DataSet'})
        
        #define the activity of aggregating two datasets(and projecting to reduce the number of unnecessary arguments)
        joinAndSimplify_lists = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(joinAndSimplify_lists, this_script)
        doc.usage(joinAndSimplify_lists, carCitations, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  }
                  )
        doc.usage(joinAndSimplify_lists, housingViolations, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  }
                  )
        
        #define the entity of the writeout
        vehicleAndHousingCitations = doc.entity('dat:aquan_erj826#vehicleAndHousingCitations', {prov.model.PROV_LABEL:'Merged and Simplified Result', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(vehicleAndHousingCitations, this_script)
        doc.wasGeneratedBy(vehicleAndHousingCitations, joinAndSimplify_lists, endTime)
        doc.wasDerivedFrom(vehicleAndHousingCitations, carCitations, joinAndSimplify_lists, joinAndSimplify_lists, joinAndSimplify_lists)
        doc.wasDerivedFrom(vehicleAndHousingCitations, housingViolations, joinAndSimplify_lists, joinAndSimplify_lists, joinAndSimplify_lists)

        repo.logout()
                  
        return doc

## eof
