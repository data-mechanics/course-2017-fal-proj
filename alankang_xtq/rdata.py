import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class rData(dml.Algorithm):
    contributor = 'alankang_xtq'
    reads = []
    writes = ['alankang_xtq.trafficSignals','alankang_xtq.hubwayStation','alankang_xtq.crash','alankang_xtq.schools']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        # url = 'https://data.cityofboston.gov/api/views/yqgx-2ktq/rows.json?accessType=DOWNLOAD'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("jam")
        # repo.createCollection("jam")
        # repo['alankang_xtq.jam'].insert_many(r)
        # repo['alankang_xtq.jam'].metadata({'complete':True})
        # #print(repo['alankang_xtq.jam'].metadata())

        url = 'http://datamechanics.io/data/alankang_xtq/Traffic_Signals.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trafficSignals")
        repo.createCollection("trafficSignals")
        #repo['alankang_xtq.trafficSignals'].insert_many(r)
        #print(repo['alankang_xtq.trafficSignals'].metadata())


        # url = 'http://datamechanics.io/data/alankang_xtq/Hubway_Stations.geojson'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("hubwayStation")
        # repo.createCollection("hubwayStation")
        # repo['alankang_xtq.hubwayStation'].insert_many(r)
        # #repo['alankang_xtq.hubwayStation'].metadata({'complete':True})
        # #print(repo['alankang_xtq.hubwayStation'].metadata())

        # url = 'http://datamechanics.io/data/alankang_xtq/crash.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("crash")
        # repo.createCollection("crash")
        # repo['alankang_xtq.crash'].insert_many(r)
        # #repo['alankang_xtq.crash'].metadata({'complete':True})
        # #print(repo['alankang_xtq.crash'].metadata())

        # url = 'http://datamechanics.io/data/alankang_xtq/Colleges_and_Universities.geojson'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("schools")
        # repo.createCollection("schools")
        # repo['alankang_xtq.schools'].insert_many(r)
        # #repo['alankang_xtq.schools'].metadata({'complete':True})
        # #print(repo['alankang_xtq.schools'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('cob', 'https://data.cityofboston.gov/api/views/yqgx-2ktq/')
        doc.add_namespace('dio', 'http://datamechanics.io/data/alankang_xtq/')
        

        # look url, base, query
        this_script = doc.agent('alg:alankang_xtq#rData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # resource_jam = doc.entity('cob:rows.json?accessType=DOWNLOAD', {'prov:label':'Traffic Jam', prov.model.PROV_TYPE:'ont:DataResource'})
        resource_trafficSignals = doc.entity('dio:Traffic_Signals', {'prov:label':'Traffic Signal', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        # resource_hubwayStation = doc.entity('dio:Hubway_Stations', {'prov:label':'Hubway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        # resource_crash = doc.entity('dio:crash', {'prov:label':'Crash', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # resource_schools = doc.entity('dio:Colleges_and_Universities', {'prov:label':'Colleges and Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        
        # get_jam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficSignals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # get_hubwayStation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # get_crash = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        # doc.wasAssociatedWith(get_jam, this_script)
        doc.wasAssociatedWith(get_trafficSignals, this_script)
        # doc.wasAssociatedWith(get_hubwayStation, this_script)
        # doc.wasAssociatedWith(get_crash, this_script)   
        # doc.wasAssociatedWith(get_schools, this_script)

        # doc.usage(get_jam, resource_jam, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_trafficSignals, resource_trafficSignals, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        # doc.usage(get_hubwayStation, resource_hubwayStation, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        # doc.usage(get_crash, resource_crash, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        # doc.usage(get_schools, resource_schools, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',})

        # jam = doc.entity('dat:alankang_xtq#jam', {prov.model.PROV_LABEL:'jam', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(jam, this_script)
        # doc.wasGeneratedBy(jam, get_jam, endTime)
        # doc.wasDerivedFrom(jam, resource_jam, get_jam, get_jam, get_jam)

        trafficSignals = doc.entity('dat:alankang_xtq#trafficSignals', {prov.model.PROV_LABEL:'trafficSignals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficSignals, this_script)
        doc.wasGeneratedBy(trafficSignals, get_trafficSignals, endTime)
        doc.wasDerivedFrom(trafficSignals, resource_trafficSignals, get_trafficSignals, get_trafficSignals, get_trafficSignals)

        # hubwayStation = doc.entity('dat:alankang_xtq#hubwayStation', {prov.model.PROV_LABEL:'hubwayStation', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(hubwayStation, this_script)
        # doc.wasGeneratedBy(hubwayStation, get_hubwayStation, endTime)
        # doc.wasDerivedFrom(hubwayStation, resource_hubwayStation, get_hubwayStation, get_hubwayStation, get_hubwayStation)

        # crash = doc.entity('dat:alankang_xtq#crash', {prov.model.PROV_LABEL:'crash', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(crash, this_script)
        # doc.wasGeneratedBy(crash, get_crash, endTime)
        # doc.wasDerivedFrom(crash, resource_crash, get_crash, get_crash, get_crash)

        # schools = doc.entity('dat:alankang_xtq#schools', {prov.model.PROV_LABEL:'schools', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(schools, this_script)
        # doc.wasGeneratedBy(schools, get_schools, endTime)
        # doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)

        repo.logout()

        return doc


## eof