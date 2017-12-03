import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getDatasets(dml.Algorithm):
    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = []
    writes = ['jdbrawn_jliang24_slarbi_tpotye.colleges', 'jdbrawn_jliang24_slarbi_tpotye.crime', 'jdbrawn_jliang24_slarbi_tpotye.crash', 'jdbrawn_jliang24_slarbi_tpotye.mbta', 'jdbrawn_jliang24_slarbi_tpotye.food', 'jdbrawn_jliang24_slarbi_tpotye.entertain', 'jdbrawn_jliang24_slarbi_tpotye.police']

    @staticmethod
    def execute(trial = False):
        """Retrieve the college location data from Analyze Boston"""
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        # Get college data
        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=208dc980-a278-49e3-b95b-e193bb7bb6e4&limit=80'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("colleges")
        repo.createCollection("colleges")
        repo['jdbrawn_jliang24_slarbi_tpotye.colleges'].insert_many(r['result']['records'])

        # Get crime data
        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=50000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['jdbrawn_jliang24_slarbi_tpotye.crime'].insert_many(r['result']['records'])

        # Get crash data
        url = 'http://datamechanics.io/data/jdbrawn_slarbi/CarCrashData.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crash")
        repo.createCollection("crash")
        repo['jdbrawn_jliang24_slarbi_tpotye.crash'].insert_many(r)

        # Get MBTA bus stop data
        url = 'http://datamechanics.io/data/jdbrawn_slarbi/MBTA_Bus_Stops.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbta")
        repo.createCollection("mbta")
        repo['jdbrawn_jliang24_slarbi_tpotye.mbta'].insert_many(r)

        #ENTERTAINMENT DATA
        url = 'https://data.cityofboston.gov/resource/cz6t-w69j.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("entertain")
        repo.createCollection("entertain")
        repo['jdbrawn_jliang24_slarbi_tpotye.entertain'].insert_many(r)

        #FOOD LICENSE DATA
        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        a = json.loads(response)
        b = json.dumps(a, sort_keys=True, indent=2)
        repo.dropCollection("food")
        repo.createCollection("food")
        repo['jdbrawn_jliang24_slarbi_tpotye.food'].insert_many(a)

        #Get police data
        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("police")
        repo.createCollection("police")
        repo['jdbrawn_jliang24_slarbi_tpotye.police'].insert_many(r)

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
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_slarbi/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#getData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_colleges = doc.entity('bdp:208dc980-a278-49e3-b95b-e193bb7bb6e4', {'prov:label':'Boston Universities and Colleges', prov.model.PROV_TYPE:'ont:DataResource'})
        resource_crime = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Boston Crime', prov.model.PROV_TYPE:'ont:DataResource'})
        resource_crashes = doc.entity('591:CarCrashData', {'prov:label':'Boston Crashes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_mbta = doc.entity('591:MBTA_Bus_Stops', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_entertain = doc.entity('bdp1:cz6t-w69j', {'prov:label':'Entertainment Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_food = doc.entity('bdp1:fdxy-gydq', {'prov:label':'Food License Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_police= doc.entity('bdp1:pyxn-r3i2' , {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_colleges = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_crime = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_crashes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_mbta = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_entertainment_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_food_license = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_police= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)


        doc.wasAssociatedWith(get_colleges, this_script)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_crashes, this_script)
        doc.wasAssociatedWith(get_mbta, this_script)
        doc.wasAssociatedWith(get_entertainment_data, this_script)
        doc.wasAssociatedWith(get_food_license, this_script)
        doc.wasAssociatedWith(get_police, this_script)


        doc.usage(get_colleges, resource_colleges, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query':'&limit=80'})
        doc.usage(get_crime, resource_crime, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query':'&limit=50000'})
        doc.usage(get_crashes, resource_crashes, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_mbta, resource_mbta, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_entertainment_data, resource_entertain, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(get_food_license, resource_food, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(get_police, resource_police, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})

        colleges = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#colleges', {prov.model.PROV_LABEL: 'Boston Universities and Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(colleges, this_script)
        doc.wasGeneratedBy(colleges, get_colleges, endTime)
        doc.wasDerivedFrom(colleges, resource_colleges, get_colleges, get_colleges, get_colleges)

        crime = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#crime', {prov.model.PROV_LABEL: 'Boston Crime', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_crime, get_crime, get_crime, get_crime)

        crash = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#crashes', {prov.model.PROV_LABEL: 'Boston Crashes', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash, this_script)
        doc.wasGeneratedBy(crash, get_crashes, endTime)
        doc.wasDerivedFrom(crash, resource_crashes, get_crashes, get_crashes, get_crashes)

        mbta = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#mbta', {prov.model.PROV_LABEL: 'MBTA Bus Stops', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_mbta, endTime)
        doc.wasDerivedFrom(mbta, resource_mbta, get_mbta, get_mbta, get_mbta)

        entertainment_data = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#entertain', {prov.model.PROV_LABEL:'Entertainment Data', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(entertainment_data, this_script)
        doc.wasGeneratedBy(entertainment_data, get_entertainment_data, endTime)
        doc.wasDerivedFrom(entertainment_data, resource_entertain, get_entertainment_data, get_entertainment_data, get_entertainment_data)

        food_license = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#food', {prov.model.PROV_LABEL:'Food License Data', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(food_license, this_script)
        doc.wasGeneratedBy(food_license, get_food_license, endTime)
        doc.wasDerivedFrom(food_license, resource_food, get_food_license, get_food_license, get_food_license)

        police = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#police', {prov.model.PROV_LABEL:'Police Data', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(police, this_script)
        doc.wasGeneratedBy(police, get_police, endTime)
        doc.wasDerivedFrom(police, resource_police, get_police, get_police, get_police)

        repo.logout()

        return doc
