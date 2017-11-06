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
Selection, projection, and aggregation on repo.aquan_erj826.Counts911, 
and an aggregation with repo.aquan_erj826.crimes to find 
the number of 911 calls that did not correspond to crime 
incident reports in 2014
"""

import json
import dml
import prov.model
import datetime
import uuid

class crimeDispatch(dml.Algorithm):
    contributor = 'aquan_erj826'
    reads = ['aquan_erj826.Counts911', 'aquan_erj826.crimes']
    writes = ['aquan_erj826.crimeDispatchResult']


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aquan_erj826', 'aquan_erj826')
        

        #This transformation will involve 911 call totals and crime reports from Boston
        dispatch = repo.aquan_erj826.Counts911
        crimeReports = repo.aquan_erj826.crimes

        repo.dropCollection("crimeDispatch")
        repo.createCollection("crimeDispatch")

        for call in dispatch.find():
            #Format the date so that it matches the format in the other collection
            date = call['Date'].split('/')
            date = date[0] + '-' + date[1] + '-' + date[2]
            total = call['Total']
            #Select data from 2014
            if date[-4::] == '2014':
                repo['aquan_erj826.crimeDispatch'].insert([{date:total}], check_keys=False)

        crimeDispatch = repo.aquan_erj826.crimeDispatch

        dates = []
        for report in crimeReports.find():
            #Adding an arbitrary year to the date to aid in selection. 
            #Note that the data sets do not overlap. For the sake of the 
            #transformations we are assuming that the data comes from the 
            #same year, 2014
            try:
                date = report['OCCURRED_ON_DATE'].split('T')[0][5:] + '-2014'
                dates.append(date)
            except:
                continue

        for point in crimeDispatch.find():
            date = list(point.keys())[1]
            total = point[date]

            #If there is a crime report that coincides with a 911 call by date,
            #subtract one from total 911 reports from that day
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
        this_script = doc.agent('alg:aquan_erj826#crimeDispatch', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the crimeList collection(entity)
        Counts911 = doc.entity('dat:aquan_erj826#Counts911', {'prov:label':'911 Collection', prov.model.PROV_TYPE:'ont:DataSet'})
        
        #define the 911Counts collection(entity)
        crimes = doc.entity('dat:aquan_erj826#crimes', {'prov:label':'Crime Collection', prov.model.PROV_TYPE:'ont:DataSet'})
        
        #define the activity of calling the script                              
        subtract_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(Counts911, this_script)
        doc.wasAssociatedWith(crimes, this_script)
        doc.usage(subtract_crimes, Counts911, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation'}
                  )
        doc.usage(subtract_crimes, crimes, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation'}
                  )

        #define the writeout entity
        crimeDispatchResult = doc.entity('dat:aquan_erj826#crimeDispatchResult', {prov.model.PROV_LABEL:'Result of Subtraction', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeDispatchResult, this_script)
        doc.wasGeneratedBy(crimeDispatchResult, subtract_crimes, endTime)
        doc.wasDerivedFrom(crimeDispatchResult, Counts911, subtract_crimes, subtract_crimes, subtract_crimes)
        doc.wasDerivedFrom(crimeDispatchResult, crimes, subtract_crimes, subtract_crimes, subtract_crimes)

        repo.logout()
                  
        return doc

## eof