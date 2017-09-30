import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fzjr_retrievalgorithm(dml.Algorithm):
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
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')

        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=homicide'
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
        repo['francisz_jrashaan.openspace'].insert_many(r)

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
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:francisz_jrashaan#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_landuse = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_capopulation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_openspace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.wasAssociatedWith(get_landuse, this_script)
        doc.wasAssociatedWith(get_capopulation, this_script)
        doc.wasAssociatedWith(get_openspace, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=crime&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_streetlights, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=streetlights&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_streetlights, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=streetlights&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_landuse, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=landuse&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_capopulation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=capopulation&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_openspace, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=openspace&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        crime = doc.entity('dat:francisz_jrashaan#crime', {prov.model.PROV_LABEL:'crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        streetlights = doc.entity('dat:francisz_jrashaan#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetlights, this_script)
        doc.wasGeneratedBy(streetlights, get_found, endTime)
        doc.wasDerivedFrom(streetlights, resource, get_streetlights, get_streetlights, get_streetlights)
        
        landuse = doc.entity('dat:francisz_jrashaan#landuse', {prov.model.PROV_LABEL:'landuse', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(landuse, this_script)
        doc.wasGeneratedBy(landuse, get_found, endTime)
        doc.wasDerivedFrom(landuse, resource, get_landuse, get_landuse, get_landuse)
        
        capopulation = doc.entity('dat:francisz_jrashaan#capopulation', {prov.model.PROV_LABEL:'capopulation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(capopulation, this_script)
        doc.wasGeneratedBy(capopulation, get_found, endTime)
        doc.wasDerivedFrom(capopulation, resource, get_capopulation, get_capopulation, get_capopulation)
        
        openspace = doc.entity('dat:francisz_jrashaan#openspace', {prov.model.PROV_LABEL:'openspace', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(openspace, this_script)
        doc.wasGeneratedBy(openspace, get_found, endTime)
        doc.wasDerivedFrom(openspace, resource, get_openspace, get_openspace, get_openspace)

        repo.logout()
                  
        return doc

fzjr_retrievalgorithm.execute()
doc = fzjr_retrievalgorithm.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
