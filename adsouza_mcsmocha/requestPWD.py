import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class requestPWD(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = []
    writes = ['adsouza_mcsmocha.PWD']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/4b0f71af07664337975119c526f5a3a8_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("PWD")
        repo.createCollection("PWD")
        repo['adsouza_mcsmocha.PWD'].insert_many(r)
        repo['adsouza_mcsmocha.PWD'].metadata({'complete':True})
        print(repo['adsouza_mcsmocha.PWD'].metadata())

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
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:adsouza_mcsmocha#requestPWD', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:4b0f71af07664337975119c526f5a3a8_2', {'prov:label':'Public Works Districts, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_pwd = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bb, this_script)
        doc.usage(get_bb, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?type=Public+Works+Districts&$PWD,NAME,COMBO,DIST,OBJECTID'
            }
            )

        pwd = doc.entity('dat:adsouza_mcsmocha#PWD', {prov.model.PROV_LABEL:'Public Works Districts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pwd, this_script)
        doc.wasGeneratedBy(pwd, get_pwd, endTime)
        doc.wasDerivedFrom(pwd, resource, get_pwd, get_pwd, get_pwd)

        repo.logout()

        return doc

requestPWD.execute()
doc = requestPWD.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))import urllib.request