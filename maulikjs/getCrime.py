import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getCrime(dml.Algorithm):
    contributor = 'maulikjs'
    reads = []
    writes = ['maulikjs.crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maulikjs', 'maulikjs')

               
        repo.dropCollection("crimes")
        repo.createCollection("crimes")

        n = 0
        while True:

            url = 'https://data.boston.gov/api/3/action/datastore_search?offset='+str(n)+'&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'  
            response = requests.get(url)
            r = response.json()['result']['records']
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo['maulikjs.crimes'].insert_many(r)

            n = n+100
            if n == 22900:
                break

        repo['maulikjs.crimes'].metadata({'complete':True})
        print(repo['maulikjs.crimes'].metadata())

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
        repo.authenticate('maulikjs', 'maulikjs')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/maulikjs#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/maulikjs#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cri', 'https://data.boston.gov/')

        this_script = doc.agent('alg:getCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cri:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Stats', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'api/3/action/datastore_search?offset=$&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'
                  }
                  )
        
        prices = doc.entity('dat:crimes', {prov.model.PROV_LABEL:'crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)

      
        repo.logout()
                  
        return doc

getCrime.execute()
doc = getCrime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
