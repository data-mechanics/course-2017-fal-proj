import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'lmy1031_zhuoshu'
    reads = []
    writes = ['lmy1031_zhuoshu_hospital']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')

        url = 'https://data.boston.gov/export/622/208/6222085d-ee88-45c6-ae40-0c7464620d64.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hospital")
        repo.createCollection("hospital")
        repo['lmy1031_zhuoshu_hospital'].insert_many(r)
        #repo['alice_bob.lost'].metadata({'complete':True})
        #print(repo['alice_bob.lost'].metadata())

        #url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        #r = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        #repo.dropCollection("found")
        #repo.createCollection("found")
        #repo['alice_bob.found'].insert_many(r)

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
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/lmy1031_zhuoshu') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/lmy1031_zhuoshu') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('hospital', 'https://data.boston.gov/')

        this_script = doc.agent('alg:#hospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('hospital:6222085d-ee88-45c6-ae40-0c7464620d64', {'prov:label':'hospital', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        licence = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(licence, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(licence, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #         'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        public = doc.entity('dat:#hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(public, this_script)
        doc.wasGeneratedBy(public, licence, endTime)
        doc.wasDerivedFrom(public, resource, licence, licence, licence)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
