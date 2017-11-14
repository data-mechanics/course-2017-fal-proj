import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import csv

class snow_emergency_routes(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = []
    writes = ['bkin18_cjoe.emergency_routes'] 

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


        # Add Snow Emergency Routes
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/4f3e4492e36f4907bcd307b131afe4a5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("emergency_routes")
        repo.createCollection("emergency_routes")
        repo['bkin18_cjoe.emergency_routes'].insert_many(r['features'])

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
        doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')


        this_script = doc.agent('alg:bkin18_cjoe#snow_emergency_routes', 
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, 
            { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        route_input = doc.entity('bdp:4f3e4492e36f4907bcd307b131afe4a5_0',
            {'prov:label':'311, Service Requests',
            prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'}) 

        output = doc.entity('dat:bkin18_cjoe.emergency_routes', 
            { prov.model.PROV_LABEL:'Snow Emergency Routes', prov.model.PROV_TYPE:'ont:DataSet'})
    
        doc.wasAssociatedWith(this_run, this_script)

        doc.used(this_run, route_input, startTime)
        # doc.usage(routes, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, route_input, this_run, this_run, this_run)

        repo.logout()

        return doc


snow_emergency_routes.execute()
doc = snow_emergency_routes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

