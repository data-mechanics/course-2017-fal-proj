import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class parkboston(dml.Algorithm):
    contributor = 'medinad'
    reads = []
    writes = ['medinad.app']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')

        url = 'https://data.boston.gov/export/8d3/8cc/8d38cc9d-8c58-462e-b2df-b793e9c05612.json'#'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("app")
        repo.createCollection("app")
        repo['medinad.app'].insert_many(r)
        repo['medinad.app'].metadata({'complete':True})
        print(repo['medinad.app'].metadata())

    
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
        repo.authenticate('medinad', 'medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        
        this_script = doc.agent('alg:medinad#parkboston', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_app = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_app, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_app, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #          'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        app = doc.entity('dat:medinad#app', {prov.model.PROV_LABEL:'APP', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(app, this_script)
        doc.wasGeneratedBy(app, get_app, endTime)
        doc.wasDerivedFrom(app, resource, get_app, get_app, get_app)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

parkboston.execute()
doc = parkboston.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
