import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getFoodViolationData(dml.Algorithm):
    contributor = 'rooday_shreyapandit'
    reads = []
    writes = ['rooday_shreyapandit.foodviolations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        # Get Food Inspection Violation data
        url = "https://data.boston.gov/api/3/action/datastore_search?resource_id=4582bec6-2b4f-4f9e-bc55-cbaa73117f4c&limit=40000"
        resp = requests.get(url).json()
        print("response has come, inserting....")
        repo.dropCollection("foodviolations")
        repo.createCollection("foodviolations")
        repo['rooday_shreyapandit.foodviolations'].insert_many(resp['result']['records'])
        repo['rooday_shreyapandit.foodviolations'].metadata({'complete':True})
        
        print("response has been inserted")
        print(repo['rooday_shreyapandit.foodviolations'].metadata())
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
        doc.add_namespace('foodviolations', 'https://data.boston.gov/dataset/food-establishment-inspections/resource/')

        this_script = doc.agent('alg:#getFoodViolationData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('foodviolations:4582bec6-2b4f-4f9e-bc55-cbaa73117f4c', {'prov:label':'Food Inspection Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_food_violation_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_food_violation_data, this_script)
        doc.usage(get_food_violation_data, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        food_violations = doc.entity('dat:#foodviolations', {prov.model.PROV_LABEL:'Food Inspection Data', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(food_violations, this_script)
        doc.wasGeneratedBy(food_violations, get_food_violation_data, endTime)
        doc.wasDerivedFrom(food_violations, resource, get_food_violation_data, get_food_violation_data, get_food_violation_data)

        repo.logout()
                  
        return doc


# getFoodViolationData.execute()
# print("running provenance for getFoodViolationData")
# doc = getFoodViolationData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))