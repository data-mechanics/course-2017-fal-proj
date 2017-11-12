"""
CS591
Project 2
11.2.17
getSFAccidents.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import random
class getSFAccidents(dml.Algorithm):
    contributor = 'alanbur_aquan_erj826_jcaluag'
    reads = []
    writes = ['alanbur_aquan_erj826_jcaluag.SFaccidents']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')          

        #Get the data set and insert it into the database
        url = 'https://data.sfgov.org/resource/vv57-2fgy.json'
        response = requests.get(url).text
        
        r = json.loads(response)

        repo.dropCollection("SFaccidents")
        repo.createCollection("SFaccidents")

        data = []
        for item in r:
            data.append({"time": item["time"], \
                      "type": item["descript"]})

        SampleSize=100
        if trial:
            TrialSample=data[:SampleSize]
            for i in range(SampleSize+1,len(data)):
                j=random.randint(1,i)
                if j<SampleSize:
                    TrialSample[j] = data[i]
            print('Running in trial mode')
            data=TrialSample


        repo['alanbur_aquan_erj826_jcaluag.SFaccidents'].insert(data, check_keys=False)
        repo['alanbur_aquan_erj826_jcaluag.SFaccidents'].metadata({'complete':True})
        print(repo['alanbur_aquan_erj826_jcaluag.SFaccidents'].metadata())

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
        doc.add_namespace('sfo', 'https://data.sfgov.org/resource/')
        
        #define the agent
        this_script = doc.agent('alg:alanbur_aquan_erj826_jcaluag#getSFAccidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource
        resource = doc.entity('sfo:vv57-2fgy', {'prov:label':'SF Accident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of taking in the resource
        get_SF_Accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_SF_Accidents, this_script)
        doc.usage(get_SF_Accidents, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        
        #define the writeout 
        accidents = doc.entity('dat:alanbur_aquan_erj826_jcaluag#getSFAccidents', {prov.model.PROV_LABEL:'SF Accident List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(accidents, this_script)
        doc.wasGeneratedBy(accidents, get_SF_Accidents, endTime)
        doc.wasDerivedFrom(accidents, resource, get_SF_Accidents, get_SF_Accidents, get_SF_Accidents)

        repo.logout()
                  
        return doc

getSFAccidents.execute(True)
## eof

