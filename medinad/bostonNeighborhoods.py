import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class bostonNeighborhoods(dml.Algorithm):
    contributor = 'medinad'
    reads = []
    writes = ['medinad.neighbor']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')

        url = 'http://datamechanics.io/data/medinad/boston-neighborhoods.json'#'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("neighbor")
        repo.createCollection("neighbor")
        repo['medinad.neighbor'].insert_many(r)
        repo['medinad.neighbor'].metadata({'complete':True})
        print(repo['medinad.neighbor'].metadata())

    
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
        doc.add_namespace('nei', 'http://datamechanics.io/data/medinad/')
        
        this_script = doc.agent('nei:medinad#bostonNeighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('nei:boston-neighborhoods', {'prov:label':'boston neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighbor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighbor, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_neighbor, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'/boston-neighborhoods.json'
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #          'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        neighbor = doc.entity('dat:medinad#neighbor', {prov.model.PROV_LABEL:'NEIGHBOR', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighbor, this_script)
        doc.wasGeneratedBy(neighbor, get_neighbor, endTime)
        doc.wasDerivedFrom(neighbor, resource, get_neighbor, get_neighbor, get_neighbor)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

bostonNeighborhoods.execute()
doc = bostonNeighborhoods.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
