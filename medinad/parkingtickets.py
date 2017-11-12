import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class parkingtickets(dml.Algorithm):
    contributor = 'medinad'
    reads = []
    writes = ['medinad.tickets']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')
        url = 'https://data.boston.gov/export/94a/c9d/94ac9d44-c6d6-441c-9c57-12dca571a02f.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("tickets")
        repo.createCollection("tickets")
        repo['medinad.tickets'].insert_many(r)
        repo['medinad.tickets'].metadata({'complete':True})
        print(repo['medinad.tickets'].metadata())

    
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
        doc.add_namespace('tic', 'https://data.boston.gov/export/94a/')
        
        this_script = doc.agent('alg:medinad#parkingtickets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'parking tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_tickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_tickets, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_tickets, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'/c9d/94ac9d44-c6d6-441c-9c57-12dca571a02f.json'
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #          'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        tickets = doc.entity('dat:medinad#tickets', {prov.model.PROV_LABEL:'TICKETS', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tickets, this_script)
        doc.wasGeneratedBy(tickets, get_tickets, endTime)
        doc.wasDerivedFrom(tickets, resource, get_tickets, get_tickets, get_tickets)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

parkingtickets.execute()
doc = parkingtickets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
