import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getVisionZeroData(dml.Algorithm):
    contributor = 'rooday_shreyapandit'
    reads = []
    writes = ['rooday_shreyapandit.visionzero']

    @staticmethod
    def execute(trial = False):
        # Retrieve some data sets (not using the API here for the sake of simplicity).
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5bed19f1f9cb41329adbafbd8ad260e5_0.geojson'
        resp = requests.get(url).json()

        print("Vision0 response has come, inserting....")

        repo.dropCollection("visionzero")
        repo.createCollection("visionzero")
        repo['rooday_shreyapandit.visionzero'].insert_many(resp['features'])
        repo['rooday_shreyapandit.visionzero'].metadata({'complete':True})

        print("response has been inserted")
        print(repo['rooday_shreyapandit.visionzero'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()
        print("Done!")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/rooday_shreyapandit') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/rooday_shreyapandit') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('visionzero', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:#getVisionZeroData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('visionzero:5bed19f1f9cb41329adbafbd8ad260e5_0', {'prov:label':'VisionZero Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_visionzero_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_visionzero_data, this_script)
        doc.usage(get_visionzero_data, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        visionzero_data = doc.entity('dat:#visionzero', {prov.model.PROV_LABEL:'VisionZero Data', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(visionzero_data, this_script)
        doc.wasGeneratedBy(visionzero_data, get_visionzero_data, endTime)
        doc.wasDerivedFrom(visionzero_data, resource, get_visionzero_data, get_visionzero_data, get_visionzero_data)

        repo.logout()
                  
        return doc

# getVisionZeroData.execute()
# print("running provenance for getVisionZeroData")
# doc = getVisionZeroData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))