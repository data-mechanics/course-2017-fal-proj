import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class rdata(dml.Algorithm):
    contributor = 'eileenli_xtq_yidingou'
    reads = []
    writes = ['eileenli_xtq_yidingou.jam', 'eileenli_xtq_yidingou.crash', 'eileenli_xtq_yidingou.schools', 'eileenli_xtq_yidingou.hubway', 'eileenli_xtq_yidingou.signals']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')

        url = 'http://datamechanics.io/data/eileenli_xtq_yidingou/Traffic_Signals.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("signals")
        repo.createCollection("signals")
        repo['eileenli_xtq_yidingou.signals'].insert_many(r)
        #repo['eileenli_xtq_yidingou.MBTA'].metadata({'complete':True})
        #print(repo['eileenli_xtq_yidingou.MBTA'].metadata())


        url = 'http://datamechanics.io/data/eileenli_xtq_yidingou/jam.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("jam")
        repo.createCollection("jam")
        repo['eileenli_xtq_yidingou.jam'].insert_many(r)
        #repo['eileenli_xtq_yidingou.jam'].metadata({'complete':True})
        #print(repo['eileenli_xtq_yidingou.jam'].metadata())

        url = 'http://datamechanics.io/data/eileenli_xtq_yidingou/Hubway_Stations.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("eileenli_xtq_yidingou.hubway")
        repo.createCollection("eileenli_xtq_yidingou.hubway")
        repo['eileenli_xtq_yidingou.hubway'].insert_many(r)
        #print(repo['eileenli_xtq_yidingou.hubway'].metadata())

        url = 'http://datamechanics.io/data/eileenli_xtq_yidingou/crash.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("eileenli_xtq_yidingou.crash")
        repo.createCollection("eileenli_xtq_yidingou.crash")
        repo['eileenli_xtq_yidingou.crash'].insert_many(r)
        #print(repo['eileenli_xtq_yidingou.crash'].metadata())

        url = 'http://datamechanics.io/data/eileenli_xtq_yidingou/Colleges_and_Universities.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['eileenli_xtq_yidingou.schools'].insert_many(r)
        #repo['eileenli_xtq_yidingou.schools'].metadata({'complete':True})
        #print(repo['eileenli_xtq_yidingou.schools'].metadata())


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
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('dio', 'http://datamechanics.io/data/eileenli_xtq_yidingou/')
        doc.add_namespace('cbg', 'https://data.cambridgema.gov/resource/')
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')


        

        # look url, base, query
        this_script = doc.agent('alg:eileenli_xtq_yidingou#rdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_jam = doc.entity('dio:jam', {'prov:label':'Traffic Jam', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_hubway = doc.entity('dio:Hubway_Stations', {'prov:label':'Hubway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_signals = doc.entity('dio:Traffic_Signals', {'prov:label':'Traffic Signal Info', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_crash = doc.entity('dio:crash', {'prov:label':'Crashes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_schools = doc.entity('dio:Colleges_and_Universities', {'prov:label':'Colleges_and_Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
       

        get_jam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_signals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crash = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hubway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_jam, this_script)
        doc.wasAssociatedWith(get_signals, this_script)
        doc.wasAssociatedWith(get_crash, this_script)
        doc.wasAssociatedWith(get_hubway, this_script)
        doc.wasAssociatedWith(get_schools, this_script)

        doc.usage(get_jam, resource_jam, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_signals, resource_signals, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_crash, resource_crash, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_hubway, resource_hubway, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_schools, resource_schools, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',})


        jam = doc.entity('dat:eileenli_xtq_yidingou#jam', {prov.model.PROV_LABEL:'jam', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(jam, this_script)
        doc.wasGeneratedBy(jam, get_jam, endTime)
        doc.wasDerivedFrom(jam, resource_jam, get_jam, get_jam, get_jam)

        signals = doc.entity('dat:eileenli_xtq_yidingou#signals', {prov.model.PROV_LABEL:'signals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(signals, this_script)
        doc.wasGeneratedBy(signals, get_signals, endTime)
        doc.wasDerivedFrom(signals, resource_signals, get_signals, get_signals, get_signals)

        crash = doc.entity('dat:eileenli_xtq_yidingou#crash', {prov.model.PROV_LABEL:'Crashes in Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crash, this_script)
        doc.wasGeneratedBy(crash, get_crash, endTime)
        doc.wasDerivedFrom(crash, resource_crash, get_crash, get_crash, get_crash)

        hubway = doc.entity('dat:eileenli_xtq_yidingou#hubway', {prov.model.PROV_LABEL:'hubway Station', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_hubway, endTime)
        doc.wasDerivedFrom(hubway, resource_hubway, get_hubway, get_hubway, get_hubway)

        schools = doc.entity('dat:eileenli_xtq_yidingou#schools', {prov.model.PROV_LABEL:'schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)


        repo.logout()

        return doc


## eof