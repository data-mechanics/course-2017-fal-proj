import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb

class readfile(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = []
    writes = ['bkin18_cjoe.emergency_routes', 'bkin18_cjoe.traffic_signals', 'bkin18_cjoe.neighborhoods'] 

    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('alice_bob', 'alice_bob')

    @staticmethod 
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe') # should probably move this to auth


        # Add Snow Emergecy Routes
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/4f3e4492e36f4907bcd307b131afe4a5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("emergency_routes")
        repo.createCollection("emergency_routes")
        repo['bkin18_cjoe.emergency_routes'].insert_many(r['features'])


        # Add Traffic Signals
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("traffic_signals")
        repo.createCollection("traffic_signals")
        repo['bkin18_cjoe.traffic_signals'].insert_many(r['features'])

        

        # Add City Building Data
#        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/492746f09dde475285b01ae7fc95950e_1.geojson' 
#        response = urllib.request.urlopen(url).read().decode("utf-8")
#        r = json.loads(response)
#        s = json.dumps(r, sort_keys=True, indent=2)
#
#        repo.dropCollection("buildings")
#        repo.createCollection("buildings")
#        repo['bkin18_cjoe.buildings'].insert_many(r['features'])


         # Add Neighborhoods
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
        repo['bkin18_cjoe.neighborhoods'].insert_many(r['features'])

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
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')


        this_script = doc.agent('alg:bkin18_cjoe#readfile', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        emergency_routes = doc.entity('bdp:4f3e4492e36f4907bcd307b131afe4a5_0',
            {'prov:label':'311, Service Requests',
            prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'})


        traffic_signals = doc.entity('bdp:de08c6fe69c942509089e6db98c716a3_0',
            {'prov:label':'311, Service Requests',
            prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'})


        neighborhoods = doc.entity('bdp:3525b0ee6e6b427f9aab5d0a1d0a1a28_0.',
            {'prov:label':'311, Service Requests',
            prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'})
        

        repo.logout()


        return doc


readfile.execute()
doc = readfile.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

