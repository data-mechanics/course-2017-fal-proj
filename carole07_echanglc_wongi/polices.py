import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class polices(dml.Algorithm):
    contributor = 'carole07_echanglc_wongi'
    reads = []
    writes = ['carole07_echanglc_wongi.polices']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Police Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("polices")
        repo.createCollection("polices")
        repo['carole07_echanglc_wongi.polices'].insert_one(r)
        print('Load polices')
        for entry in repo.carole07_echanglc_wongi.polices.find():
             print(entry)
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def execute(trial = True):
        '''Retrieve Police Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("polices")
        repo.createCollection("polices")
        repo['carole07_echanglc_wongi.polices'].insert_one(r)
        print('Load polices')
        for entry in repo.carole07_echanglc_wongi.polices.find():
            print(entry)
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
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/622/208/')

        this_script = doc.agent('alg:carole07_echanglc_wongi#polices', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:e5a0066d38ac4e2abbc7918197a4f6af_6', {'prov:label':'Boston police stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_polices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_polices, this_script)
        doc.usage(get_polices, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        polices = doc.entity('dat:carole07_echanglc_wongi#polices', {prov.model.PROV_LABEL:'Boston police stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(polices, this_script)
        doc.wasGeneratedBy(polices, get_polices, endTime)
        doc.wasDerivedFrom(polices, resource, get_polices, get_polices, get_polices)

        repo.logout()

        return doc
