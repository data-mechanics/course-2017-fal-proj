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
    writes = ['htw93_tscheung.BostonCrime', 'htw93_tscheung.CambridgeCrime',
              'htw93_tscheung.BostonSchools', 'htw93_tscheung.CambridgeSchools',
              'htw93_tscheung.BostonRestaurants', 'htw93_tscheung.CambridgeRestaurants']

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

        url = 'https://data.cambridgema.gov/resource/dypy-nwuz.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("CambridgeCrime")
        repo.createCollection("CambridgeCrime")
        repo['htw93_tscheung.CambridgeCrime'].insert_many(r)
        print('Finished rectrieving htw93_tscheung.CambridgeCrime')
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = geojson.loads(response)
        s = json.dumps(r['features'], sort_keys=True, indent=2)
        s_r = json.loads(s)
        repo.dropCollection("BostonSchools")
        repo.createCollection("BostonSchools")
        repo['htw93_tscheung.BostonSchools'].insert_many(s_r)
        print('Finished rectrieving htw93_tscheung.BostonSchools')
        
        url = 'https://data.cambridgema.gov/resource/fmjd-dgre.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("CambridgeSchools")
        repo.createCollection("CambridgeSchools")
        repo['htw93_tscheung.CambridgeSchools'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung.CambridgeSchools')

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

getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
