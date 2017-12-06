import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class camSchools(dml.Algorithm):
    contributor = 'carole07_echanglc_wongi'
    reads = []
    writes = ['carole07_echanglc_wongi.camSchools']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Employee camSchools Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        url = 'https://data.cambridgema.gov/api/views/fmjd-dgre/rows.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("camSchools")
        repo.createCollection("camSchools")
        repo['carole07_echanglc_wongi.camSchools'].insert_one(r)
        print('Load cambridge schools')
        #for entry in repo.carole07_echanglc_wongi.camSchools.find():
        #     print(entry)
             
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def execute(trial = True):
        '''Retrieve Employee camSchools Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
        
        url = 'https://data.cambridgema.gov/api/views/fmjd-dgre/rows.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("camSchools")
        repo.createCollection("camSchools")
        repo['carole07_echanglc_wongi.camSchools'].insert_one(r)
        print('Load cambridge schools')
        #for entry in repo.carole07_echanglc_wongi.camSchools.find():
        #     print(entry)
        
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
        doc.add_namespace('bdp', 'https://data.cambridgema.gov/api/views/')

        this_script = doc.agent('alg:carole07_echanglc_wongi#camSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:fmjd-dgre/rows', {'prov:label':'Cambridge Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_camSchools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_camSchools, this_script)
        doc.usage(get_camSchools, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        camSchools = doc.entity('dat:carole07_echanglc_wongi#camSchools', {prov.model.PROV_LABEL:'Cambridge Schools ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(camSchools, this_script)
        doc.wasGeneratedBy(camSchools, get_camSchools, endTime)
        doc.wasDerivedFrom(camSchools, resource, get_camSchools, get_camSchools, get_camSchools)

        repo.logout()

        return doc
