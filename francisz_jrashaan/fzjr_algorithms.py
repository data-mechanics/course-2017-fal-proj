import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fzjr_algorithms(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    reads = []
    writes = ['francisz_jrashaan.crime', 'francisz_jrashaan.cambridgepopulation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan')

        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=1000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['francisz_jrashaan.crime'].insert_many(r)
        repo['francisz_jrashaan.crime'].metadata({'complete':True})
        print(repo['francisz_jrashaan.crime'].metadata())
        
        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5&limit=1000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("streetlights")
        repo.createCollection("streetlights")
        repo['francisz_jrashaan.streetlights'].insert_many(r)
        repo['francisz_jrashaan.streetlights'].metadata({'complete':True})
        print(repo['francisz_jrashaan.streetlights'].metadata())


        url = 'https://data.cambridgema.gov/api/views/srp4-fhjz/rows.json?'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("landuse")
        repo.createCollection("landuse")
        repo['francisz_jrashaan.landuse'].insert_many(r)
        repo['francisz_jrashaan.landuse'].metadata({'complete':True})
        print(repo['francisz_jrashaan.landuse'].metadata())
        
        url = 'https://data.cambridgema.gov/api/views/r4pm-qqje/rows.json?'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("capopulation")
        repo.createCollection("capopulation")
        repo['francisz_jrashaan.capopulation'].insert_many(r)
        repo['francisz_jrashaan.capopulation'].metadata({'complete':True})
        print(repo['francisz_jrashaan.capopulation'].metadata())
        
        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=769c0a21-9e35-48de-a7b0-2b7dfdefd35e'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("openspace")
        repo.createCollection("openspace")
        repo['francisz_jrashaan'.found'].insert_many(r)

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

fzjr_algorithms.execute()
doc = fzjr_algorithms.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
