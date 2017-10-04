import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getServiceRequestsData(dml.Algorithm):
    contributor = 'rooday_shreyapandit'
    reads = []
    writes = ['rooday_shreyapandit.servicerequests']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        # Get 311 data
        url = "https://data.boston.gov/api/action/datastore_search_sql?sql=SELECT%20*%20from%20%222968e2c0-d479-49ba-a884-4ef523ada3c0%22%20WHERE%20open_dt%20%3E%20%272016-01-01%27"
        resp = requests.get(url).json()
        print("response has come, inserting....")
        repo.dropCollection("servicerequests")
        repo.createCollection("servicerequests")
        repo['rooday_shreyapandit.servicerequests'].insert_many(resp['result']['records'])
        repo['rooday_shreyapandit.servicerequests'].metadata({'complete':True})
        print("response has been inserted")
        print(repo['rooday_shreyapandit.servicerequests'].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        print("Done!")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("***Doc is")
        print(doc)
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/rooday_shreyapandit') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/rooday_shreyapandit') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('servicerequests', 'https://data.boston.gov/dataset/311-service-requests/resource/')

        this_script = doc.agent('alg:#getServiceRequestsData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('servicerequests:2968e2c0-d479-49ba-a884-4ef523ada3c0', {'prov:label':'Service Requests Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_service_requests_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_service_requests_data, this_script)
        doc.usage(get_service_requests_data, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        service_requests = doc.entity('dat:#servicerequests', {prov.model.PROV_LABEL:'Service Requests Data', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(service_requests, this_script)
        doc.wasGeneratedBy(service_requests, get_service_requests_data, endTime)
        doc.wasDerivedFrom(service_requests, resource, get_service_requests_data, get_service_requests_data, get_service_requests_data)

        repo.logout()
                  
        return doc

#getServiceRequestsData.execute()
#print("running provenance for getServiceRequestsData")
#doc = getServiceRequestsData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))