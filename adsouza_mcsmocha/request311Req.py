import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class request311Req(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = []
    writes = ['adsouza_mcsmocha.ThreeReq']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        url = 'https://data.boston.gov/export/296/8e2/2968e2c0-d479-49ba-a884-4ef523ada3c0.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace("]", "")
        response += "]"
        # print(type(response))
        r = json.loads(response)
        # print(type(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(type(s))
        repo.dropCollection("ThreeReq")
        repo.createCollection("ThreeReq")
        repo['adsouza_mcsmocha.ThreeReq'].insert_many(r)
        repo['adsouza_mcsmocha.ThreeReq'].metadata({'complete':True})
        print(repo['adsouza_mcsmocha.ThreeReq'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')
       	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        # Additional resource
        doc.add_namespace('anb', 'https://data.boston.gov/')

        this_script = doc.agent('alg:adsouza_mcsmocha#request311Req', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('anb:2968e2c0-d479-49ba-a884-4ef523ada3c0', {'prov:label':'311 Requests, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_311, this_script)
        doc.usage(get_311, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval',
        	'ont:Query':'?type=311+Requests&$CASE_TITLE,TYPE,QUEUE,Department,Location,pwd_district,neighborhood,neighborhood_services_district,LOCATION_STREET_NAME,LOCATION_ZIPCODE'
        	}
        	)

        req_311 = doc.entity('dat:adsouza_mcsmocha#ThreeReq', {prov.model.PROV_LABEL:'311 Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(req_311, this_script)
        doc.wasGeneratedBy(req_311, get_311, endTime)
        doc.wasDerivedFrom(req_311, resource, get_311, get_311, get_311)

        repo.logout()

        return doc

request311Req.execute()
doc = request311Req.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))