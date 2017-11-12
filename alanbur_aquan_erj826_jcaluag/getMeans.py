"""
CS591
Project 2
11.2.17
getMeanAndStdev.py
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getComplaints(dml.Algorithm):
    contributor = 'alanbur_aquan_erj826_jcaluag'
    reads = ['alanbur_aquan_erj826_jcaluag.parseaccidents']
    writes = ['alanbur_aquan_erj826_jcaluag.meanAndStdev']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime incident report information from Boston.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('alanbur_aquan_erj826_jcaluag', 'alanbur_aquan_erj826_jcaluag')          

        # collection = repo.alanbur_aquan_erj826_jcaluag.parseaccidents
        collection = repo['alanbur_aquan_erj826_jcaluag.parseaccidents'].find()

        repo.dropCollection("alanbur_aquan_erj826_jcaluag.meanAndStdev")
        repo.createCollection("alanbur_aquan_erj826_jcaluag.meanAndStdev")

        SampleSize=1000


        if trial:
            TrialSample=collection[:SampleSize]
            for i in range(SampleSize+1,len(collection)):
                j=random.randint(1,i)
                if j<kSize:
                    TrialSample[j] = self.Allreviews[i]
            print('Running in trial mode')
            collection=TrialSample

        print(len(collection))
        timeSum=0
        casualtySum=0
        itemCount=0
        for entry in collection:
            #sum the casualties
            casualtySum+=entry['total_casualties']
            
            #sum the times, by minute
            timeEntry = entry['time']
            data = timeEntry.split(':')
            hourMins = int(data[0]) * 60
            dataSum = hourMins + int(data[1])
            timeSum +=dataSum
            
            #count the entries
            itemCount+=1

        #average calculations    
        avgCasualties = casualtySum/itemCount
        avgTimeMins = timeSum/itemCount
        theTrueHour = (int)(avgTimeMins/60)
        theTrueMinute= avgTimeMins%60
        avgTime=str(theTrueHour) + ":" + str(theTrueMinute)  
        
        n = {} #the single entry of our dataset that contains statistics
        n['avgCasualties'] = avgCasualties
        n['avgTime'] = avgTime

        #lets calculate stdev and 


        repo['alanbur_aquan_erj826_jcaluag.meanAndStdev'].insert(n, check_keys=False)


        repo['alanbur_aquan_erj826_jcaluag.meanAndStdev'].metadata({'complete':True})
        print(repo['alanbur_aquan_erj826_jcaluag.meanAndStdev'].metadata())

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('dbe', 'https://data.boston.gov/export/245/954/')
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

getComplaints.execute()

## eof
