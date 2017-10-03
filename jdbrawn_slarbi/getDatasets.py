import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getDatasets(dml.Algorithm):
    contributor = 'jdbrawn_slarbi'
    reads = []
    writes = ['jdbrawn_slarbi.colleges', 'jdbrawn_slarbi.crime', 'jdbrawn_slarbi.crash', 'jdbrawn_slarbi.mbta']

    @staticmethod
    def execute(trial = False):
        """Retrieve the college location data from Analyze Boston"""
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        # Get college data
        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=208dc980-a278-49e3-b95b-e193bb7bb6e4&limit=80'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("colleges")
        repo.createCollection("colleges")
        repo['jdbrawn_slarbi.colleges'].insert_many(r['result']['records'])

        # Get crime data
        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=10000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['jdbrawn_slarbi.crime'].insert_many(r['result']['records'])

        # Get crash data
        url = 'http://datamechanics.io/data/jdbrawn_slarbi/CarCrashData.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crash")
        repo.createCollection("crash")
        repo['jdbrawn_slarbi.crash'].insert_many(r)

        # Get MBTA bus stop data
        url = 'http://datamechanics.io/data/jdbrawn_slarbi/MBTA_Bus_Stops.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbta")
        repo.createCollection("mbta")
        repo['jdbrawn_slarbi.mbta'].insert_many(r)

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
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_slarbi/')

        this_script = doc.agent('alg:jdbrawn_slarbi#getData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_colleges = doc.entity('bdp:208dc980-a278-49e3-b95b-e193bb7bb6e4&limit=80', {'prov:label':'Boston Universities and Colleges', prov.model.PROV_TYPE:'ont:DataResource'})
        resource_crime = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=10000', {'prov:label':'Boston Crime', prov.model.PROV_TYPE:'ont:DataResource'})
        resource_crashes = doc.entity('591:CarCrashData', {'prov:label':'Boston Crashes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_mbta = doc.entity('591:MBTA_Bus_Stops', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})

        get_colleges = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_crime = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_crashes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_mbta = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_colleges, this_script)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_crashes, this_script)
        doc.wasAssociatedWith(get_mbta, this_script)

        doc.usage(get_colleges, resource_colleges, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_crime, resource_crime, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_crashes, resource_crashes, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_mbta, resource_mbta, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        colleges = doc.entity('dat:jdbrawn_slarbi#colleges', {prov.model.PROV_LABEL: 'Boston Universities and Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(colleges, this_script)
        doc.wasGeneratedBy(colleges, get_colleges, endTime)
        doc.wasDerivedFrom(colleges, resource_colleges, get_colleges, get_colleges, get_colleges)

        crime = doc.entity('dat:jdbrawn_slarbi#crime', {prov.model.PROV_LABEL: 'Boston Crime', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_crime, get_crime, get_crime, get_crime)

        crash = doc.entity('dat:jdbrawn_slarbi#crashes', {prov.model.PROV_LABEL: 'Boston Crashes', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash, this_script)
        doc.wasGeneratedBy(crash, get_crashes, endTime)
        doc.wasDerivedFrom(crash, resource_crashes, get_crashes, get_crashes, get_crashes)

        mbta = doc.entity('dat:jdbrawn_slarbi#mbta', {prov.model.PROV_LABEL: 'MBTA Bus Stops', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_mbta, endTime)
        doc.wasDerivedFrom(mbta, resource_mbta, get_mbta, get_mbta, get_mbta)

        repo.logout()

        return doc
