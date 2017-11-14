"""
CS591
Project 2
11.2.17
timeAnalyzer.py

reads the two collections with [time, accident] data
returns correlation coefficient
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import numpy as np

class CalculateCorrelation(dml.Algorithm):
    contributor = 'alanbur_aquan_erj826_jcaluag'
    reads = ['alanbur_aquan_erj826_jcaluag.boroughAggregateNY', 'alanbur_aquan_erj826_jcaluag.timeAggregateSF','alanbur_aquan_erj826_jcaluag.timeAggregateNY']
    writes = ['alanbur_aquan_erj826_jcaluag.correlation']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        print("Building 6 by 6 correlation matrix")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')          

        ny = [entry for entry in repo.alanbur_aquan_erj826_jcaluag.boroughAggregateNY.find()][0]
        sf = [entry['data'] for entry in repo.alanbur_aquan_erj826_jcaluag.timeAggregateSF.find()][0]


        NYall = [entry['data'] for entry in repo.alanbur_aquan_erj826_jcaluag.timeAggregateNY.find()][0]

        m= ny['MANHATTAN']
        b = ny['BROOKLYN']
        q = ny['QUEENS']
        bronx = ny['BRONX']
        s = ny['STATEN ISLAND']
        cov = np.corrcoef([NYall,sf,m,b,q,bronx,s])

        print(cov)


        


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
        this_script = doc.agent('alg:alanbur_aquan_erj826_jcaluag#CalculateCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        
        #define the input resource
        resource = doc.entity('dat:boroughAggregateNY', {'prov:label':'NY Parsed Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('dat:timeAggregateSF', {'prov:label':'NY Time Aggregated Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('dat:timeAggregateNY', {'prov:label':'SF Time Aggregated Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #define the activity of taking in the resource
        action = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(action, this_script)
        doc.usage(action, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(action, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(action, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        
        #define the writeout 
        output = doc.entity('dat:alanbur_aquan_erj826_jcaluag#correlation', {prov.model.PROV_LABEL:'The Correlation Matrix', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, action, endTime)
        doc.wasDerivedFrom(output, resource, action, action, action)

        repo.logout()
                  
        return doc

# CalculateCorrelation.execute()

## eof
