"""
CS591
Project 2
11.2.17
getNYAccidents.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getNYAccidents(dml.Algorithm):
    contributor = 'alanbur_aquan_erj826_jcaluag'
    reads = []
    writes = ['alanbur_aquan_erj826_jcaluag.NYaccidents']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')          

        #Get the data set and insert it into the database
        url = 'https://data.cityofnewyork.us/resource/qiz3-axqb.json'
        response = requests.get(url).text
        
        data = json.loads(response)

        SampleSize=100
        if trial:
            TrialSample=data[:SampleSize]
            for i in range(SampleSize+1,len(data)):
                j=random.randint(1,i)
                if j<SampleSize:
                    TrialSample[j] = data[i]
            print('Running in trial mode')
            data=TrialSample



        repo.dropCollection("NYaccidents")
        repo.createCollection("NYaccidents")
        repo['alanbur_aquan_erj826_jcaluag.NYaccidents'].insert(data, check_keys=False)
        repo['alanbur_aquan_erj826_jcaluag.NYaccidents'].metadata({'complete':True})
        print(repo['alanbur_aquan_erj826_jcaluag.NYaccidents'].metadata())

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
        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        #resources:
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')
        
        #define the agent
        this_script = doc.agent('alg:alanbur_aquan_erj826_jcaluag#getNYAccidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource
        resource = doc.entity('nyc:qiz3-axqb', {'prov:label':'NY Accident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of taking in the resource
        get_NY_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_NY_accidents, this_script)
        doc.usage(get_NY_accidents, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        
        #define the writeout 
        accidents = doc.entity('dat:alanbur_aquan_erj826_jcaluag#NYaccidents', {prov.model.PROV_LABEL:'NY Accidents List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(accidents, this_script)
        doc.wasGeneratedBy(accidents, get_NY_accidents, endTime)
        doc.wasDerivedFrom(accidents, resource, get_NY_accidents, get_NY_accidents, get_NY_accidents)

        repo.logout()
                  
        return doc

getNYAccidents.execute()
## eof

