import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class rdata(dml.Algorithm):
    contributor = 'alankang_xtq'
    reads = []
    writes = ['alankang_xtq.jam', 'alankang_xtq.trafficsignals','alankang_xtq.hubwaystation','alankang_xtq.crash','alankang_xtq.schools']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        # url = 'https://data.cityofboston.gov/api/views/yqgx-2ktq/rows.json?accessType=DOWNLOAD'
        # response = requests.get(url).text
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("jam")
        # repo.createCollection("jam")
        # repo['alankang_xtq.jam'].insert_many(r)
        ##repo['alankang_xtq.jam'].metadata({'complete':True})
        ##print(repo['alankang_xtq.jam'].metadata())

        url = 'http://datamechanics.io/data/Traffic_Signals.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trafficsignals")
        repo.createCollection("trafficsignals")
        ##repo['alankang_xtq.trafficsignals'].insert_many(r)
        ##print(repo['alankang_xtq.trafficsignals'].metadata())

        url = 'http://datamechanics.io/data/Hubway_Stations%20(1).geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubwaystation")
        repo.createCollection("hubwaystation")
        repo['alankang_xtq.hubwaystation'].insert_many(r)
        ##repo['alankang_xtq.hubwaystation'].metadata({'complete':True})
        ##print(repo['alankang_xtq.hubwaystation'].metadata())

        url = 'http://datamechanics.io/data/crash.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crash")
        repo.createCollection("crash")
        repo['alankang_xtq.crash'].insert_many(r)
        ##repo['alankang_xtq.crash'].metadata({'complete':True})
        ##print(repo['alankang_xtq.crash'].metadata())

        url = 'http://datamechanics.io/data/Colleges_and_Universities.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['alankang_xtq.schools'].insert_many(r)
        ##repo['alankang_xtq.schools'].metadata({'complete':True})
        ##print(repo['alankang_xtq.schools'].metadata())

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
        repo.authenticate('alankang_xtq', 'alankang_xtq')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alankang_xtq#rdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_jam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficsignals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hubwaystation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crash = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_jam, this_script)
        doc.wasAssociatedWith(get_trafficsignals, this_script)
        doc.wasAssociatedWith(get_hubwaystation, this_script)
        doc.wasAssociatedWith(get_crash, this_script)   
        doc.wasAssociatedWith(get_schools, this_script)
        
        doc.usage(get_jam, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=jam&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_trafficsignals, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=trafficsignals&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_hubwaystation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=hubwaystation&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_crash, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=crash&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_schools, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=schools&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        jam = doc.entity('dat:alankang_xtq#jam', {prov.model.PROV_LABEL:'jam', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(jam, this_script)
        doc.wasGeneratedBy(jam, get_jam, endTime)
        doc.wasDerivedFrom(jam, resource, get_jam, get_jam, get_jam)

        trafficsignals = doc.entity('dat:alankang_xtq#trafficsignals', {prov.model.PROV_LABEL:'trafficsignals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficsignals, this_script)
        doc.wasGeneratedBy(trafficsignals, get_trafficsignals, endTime)
        doc.wasDerivedFrom(trafficsignals, resource, get_trafficsignals, get_trafficsignals, get_trafficsignals)

        hubwaystation = doc.entity('dat:alankang_xtq#hubwaystation', {prov.model.PROV_LABEL:'hubwaystation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwaystation, this_script)
        doc.wasGeneratedBy(hubwaystation, get_hubwaystation, endTime)
        doc.wasDerivedFrom(hubwaystation, resource, get_hubwaystation, get_hubwaystation, get_hubwaystation)

        crash = doc.entity('dat:alankang_xtq#crash', {prov.model.PROV_LABEL:'crash', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crash, this_script)
        doc.wasGeneratedBy(crash, get_crash, endTime)
        doc.wasDerivedFrom(crash, resource, get_crash, get_crash, get_crash)

        schools = doc.entity('dat:alankang_xtq#schools', {prov.model.PROV_LABEL:'schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource, get_schools, get_schools, get_schools)

        repo.logout()
                  
        return doc

rdata.execute()
doc = rdata.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
