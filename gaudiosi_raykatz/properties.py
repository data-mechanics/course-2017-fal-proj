import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class properties(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = []
    writes = ['gaudiosi_katz.properties']

    @staticmethod
    def execute(trial = False):
        '''Retrieve properties from City of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        boston_url = "https://data.boston.gov"
        url = "https://data.boston.gov/api/action/datastore_search?offset=000000&resource_id=062fc6fa-b5ff-4270-86cf-202225e40858"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        result = json.loads(response)
        r = result["result"]["records"]
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("properties")
        repo.createCollection("properties")
        repo['gaudiosi_raykatz.properties'].insert_many(r)
        ''' If there are >100 results, need to query multiple pages 
        if result["total"] > 100 and result["offset"] < result["total"] - 100:
            total = result["total"]
            runs = total // 100
            for run in runs:
                url = boston_url + result["_links"]["next"]
                response = urllib.request.urlopen(url).read().decode("utf-8")
                result = json.loads(response)
                r = result["result"]["records"]
                s = json.dumps(r, sort_keys=True, indent=2)
                repo['gaudiosi_raykatz.properties'].insert_many(r)
        '''
        repo['gaudiosi_raykatz.properties'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.properties'].metadata())
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
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gaudiosi_raykatz#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_properties = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_properties, this_script)
        
        doc.usage(get_properties, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Property&$select=MAIL_ADDRESS,OWNER'
                  }
                  )
        
        properties = doc.entity('dat:gaudiosi_raykatz#properties', {prov.model.PROV_LABEL:'Properties', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties, this_script)
        doc.wasGeneratedBy(properties, get_properties, endTime)
        doc.wasDerivedFrom(properties, resource, get_properties, get_properties, get_properties)

        repo.logout()
                  
        return doc

properties.execute()
doc = properties.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
