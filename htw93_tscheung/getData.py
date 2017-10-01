import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson

class getData(dml.Algorithm):
    contributor = 'htw93_tscheung'
    reads = []
    writes = ['htw93_tscheung.BostonCrime', 'htw93_tscheung.NewYorkCityCrime',
              'htw93_tscheung.BostonSchools', 'htw93_tscheung.NewYorkCitySchools',
              'htw93_tscheung.BostonHospitals', 'htw93_tscheung.NewYorkCityHospitals']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung', 'htw93_tscheung')

        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BostonCrime")
        repo.createCollection("BostonCrime")
        repo['htw93_tscheung.BostonCrime'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung.BostonCrime')

        url = 'https://data.cityofnewyork.us/resource/qgea-i56i.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("NewYorkCityCrime")
        repo.createCollection("NewYorkCityCrime")
        repo['htw93_tscheung.NewYorkCityCrime'].insert_many(r)
        print('Finished rectrieving htw93_tscheung.NewYorkCityCrime')
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = geojson.loads(response)
        s = json.dumps(r['features'], sort_keys=True, indent=2)
        s_r = json.loads(s)
        repo.dropCollection("BostonSchools")
        repo.createCollection("BostonSchools")
        repo['htw93_tscheung.BostonSchools'].insert_many(s_r)
        print('Finished rectrieving htw93_tscheung.BostonSchools')
        
        url = 'https://data.cityofnewyork.us/resource/8pnn-kkif.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("NewYorkCitySchools")
        repo.createCollection("NewYorkCitySchools")
        repo['htw93_tscheung.NewYorkCitySchools'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung.NewYorkCitySchools')
        
        url = 'https://data.cityofboston.gov/resource/46f7-2snz.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BostonHospitals")
        repo.createCollection("BostonHospitals")
        repo['htw93_tscheung.BostonHospitals'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung.BostonHospitals')
        
        url = 'https://data.cityofnewyork.us/resource/ymhw-9cz9.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("NewYorkCityHospitals")
        repo.createCollection("NewYorkCityHospitals")
        repo['htw93_tscheung.NewYorkCityHospitals'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung.NewYorkCityHospitals')

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
        repo.authenticate('htw93_tscheung', 'htw93_tscheung')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('ndp', 'https://data.cityofnewyork.us/resource/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        getData_script = doc.agent('alg:htw93_tscheung#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        BostonSchoolsResource = doc.entity('bod:cbf14bb032ef4bd38e20429f71acb61a_2', {'prov:label':'Boston Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        NewYorkCitySchoolsResource = doc.entity('ndp:8pnn-kkif', {'prov:label':'New York City Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        BostonCrimeResource = doc.entity('bdp:29yf-ye7nf', {'prov:label':'Boston Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCityCrimeResource = doc.entity('ndp:qgea-i56i', {'prov:label':'New York City Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        BostonHospitalsResource = doc.entity('bdp:46f7-2snz', {'prov:label':'Boston Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCityHospitalsResource = doc.entity('ndp:ymhw-9cz9', {'prov:label':'New York City Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_BostonSchools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_NewYorkCitySchools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_BostonCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_NewYorkCityCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_BostonHospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_NewYorkCityHospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_BostonSchools, getData_script)
        doc.wasAssociatedWith(get_NewYorkCitySchools, getData_script)
        doc.wasAssociatedWith(get_BostonCrime, getData_script)
        doc.wasAssociatedWith(get_NewYorkCityCrime, getData_script)
        doc.wasAssociatedWith(get_BostonHospitals, getData_script)
        doc.wasAssociatedWith(get_NewYorkCityHospitals, getData_script)
        
        doc.usage(get_BostonSchools, BostonSchoolsResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_NewYorkCitySchools, NewYorkCitySchoolsResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_BostonCrime, BostonCrimeResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_NewYorkCityCrime, NewYorkCityCrimeResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_BostonHospitals, BostonHospitalsResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_NewYorkCityHospitals, NewYorkCityHospitalsResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})


        bostonSchools = doc.entity('dat:htw93_tscheung#BostonSchools', {prov.model.PROV_LABEL:'Boston Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonSchools, getData_script)
        doc.wasGeneratedBy(bostonSchools, get_BostonSchools, endTime)
        doc.wasDerivedFrom(bostonSchools, BostonSchoolsResource, get_BostonSchools, get_BostonSchools, get_BostonSchools)

        newYorkCitySchools = doc.entity('dat:htw93_tscheung#NewYorkCitySchools', {prov.model.PROV_LABEL:'New York City Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(newYorkCitySchools, getData_script)
        doc.wasGeneratedBy(newYorkCitySchools, get_NewYorkCitySchools, endTime)
        doc.wasDerivedFrom(newYorkCitySchools, NewYorkCitySchoolsResource, get_NewYorkCitySchools, get_NewYorkCitySchools, get_NewYorkCitySchools)
        
        bostonCrime = doc.entity('dat:htw93_tscheung#BostonCrime', {prov.model.PROV_LABEL:'Boston Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonCrime, getData_script)
        doc.wasGeneratedBy(bostonCrime, get_BostonCrime, endTime)
        doc.wasDerivedFrom(bostonCrime, BostonCrimeResource, get_BostonCrime, get_BostonCrime, get_BostonCrime)
        
        newYorkCityCrime = doc.entity('dat:htw93_tscheung#NewYorkCityCrime', {prov.model.PROV_LABEL:'New York City Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(newYorkCityCrime, getData_script)
        doc.wasGeneratedBy(newYorkCityCrime, get_NewYorkCitySchools, endTime)
        doc.wasDerivedFrom(newYorkCityCrime, NewYorkCityCrimeResource, get_NewYorkCityCrime, get_NewYorkCityCrime, get_NewYorkCityCrime)
        
        bostonHospitals = doc.entity('dat:htw93_tscheung#BostonHospitals', {prov.model.PROV_LABEL:'Boston Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonHospitals, getData_script)
        doc.wasGeneratedBy(bostonHospitals, get_BostonHospitals, endTime)
        doc.wasDerivedFrom(bostonHospitals, BostonHospitalsResource, get_BostonHospitals, get_BostonHospitals, get_BostonHospitals)
        
        newYorkCityHospitals = doc.entity('dat:htw93_tscheung#NewYorkCityHospitals', {prov.model.PROV_LABEL:'New York City Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(newYorkCityHospitals, getData_script)
        doc.wasGeneratedBy(newYorkCityHospitals, get_NewYorkCityHospitals, endTime)
        doc.wasDerivedFrom(newYorkCityHospitals, NewYorkCityHospitalsResource, get_NewYorkCityHospitals, get_NewYorkCityHospitals, get_NewYorkCityHospitals)

        repo.logout()
                  
        return doc

getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
