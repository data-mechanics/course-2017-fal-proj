import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import csv

class neighborhood_code(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = []
    writes = ['bkin18_cjoe.neighborhoods'] 

    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    # repo.authenticate('bkin18_cjoe', 'bkin18_cjoe')

    @staticmethod 
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe') # should probably move this to auth


         # Add Neighborhoods
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
        repo['bkin18_cjoe.neighborhoods'].insert_many(r['features'])

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


        this_script = doc.agent('alg:bkin18_cjoe#neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bdp:3525b0ee6e6b427f9aab5d0a1d0a1a28_0.',
            {'prov:label':'311, Service Requests',
            prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'})
        ## Work on this later
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        neighborhood_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        neighborhoods = doc.entity('dat:bkin18_cjoe#neighborhoods', {prov.model.PROV_LABEL:'Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
    
        doc.wasAssociatedWith(neighborhood_activity, this_script)
        doc.usage(neighborhood_activity, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        # doc.wasAttributedTo(found, this_script)

        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, neighborhood_activity, endTime)
        doc.wasDerivedFrom(neighborhoods, resource, this_run, this_run, this_run)

        repo.logout()


        return doc


neighborhood_code.execute()
doc = neighborhood_code.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

