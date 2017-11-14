"""
CS591
Project 2
11.2.17
parseNYAccidents.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class parseNYAccidents(dml.Algorithm):
    contributor = 'alanbur_aquan_erj826_jcaluag'
    reads = ['alanbur_aquan_erj826_jcaluag.NYaccidents']
    writes = ['alanbur_aquan_erj826_jcaluag.parseNYaccidents']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')          

        collection = repo.alanbur_aquan_erj826_jcaluag.NYaccidents

        repo.dropCollection("alanbur_aquan_erj826_jcaluag.parseNYaccidents")
        repo.createCollection("alanbur_aquan_erj826_jcaluag.parseNYaccidents")

        for entry in collection.find():
            n = {}
            try:
                n['borough'] = entry['borough']
                n['time'] = entry['time']
                n['total_casualties'] = int(entry['number_of_persons_injured']) + int(entry['number_of_persons_killed'])
                location= (entry['location'])
                n['longitude']=location['coordinates'][0]
                n['latitude']=location['coordinates'][1]
                print(n)
            except:
                continue

            repo['alanbur_aquan_erj826_jcaluag.parseNYaccidents'].insert(n, check_keys=False)


        repo['alanbur_aquan_erj826_jcaluag.parseNYaccidents'].metadata({'complete':True})
        print(repo['alanbur_aquan_erj826_jcaluag.parseNYaccidents'].metadata())

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
        
        #define the agent
        this_script = doc.agent('alg:alanbur_aquan_erj826_jcaluag#parseNYaccidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #define the input resource
        resource = doc.entity('dat:alanbur_aquan_erj826_jcaluag#parseNYaccidents', {'prov:label':'Parsed NY Accident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of taking in the resource
        get_parsed_NY_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_parsed_NY_accidents, this_script)
        doc.usage(get_parsed_NY_accidents, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        
        #define the writeout 
        parsed_accidents = doc.entity('dat:alanbur_aquan_erj826_jcaluag#parseNYaccidents', {prov.model.PROV_LABEL:'NY Accidents List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(parsed_accidents, this_script)
        doc.wasGeneratedBy(parsed_accidents, get_parsed_NY_accidents, endTime)
        doc.wasDerivedFrom(parsed_accidents, resource, get_parsed_NY_accidents, get_parsed_NY_accidents, get_parsed_NY_accidents)

        repo.logout()
                  
        return doc

# parseNYAccidents.execute()

## eof
