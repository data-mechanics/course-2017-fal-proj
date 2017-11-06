import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hospitals(dml.Algorithm):
    contributor = 'wongi'
    reads = []
    writes = ['wongi.hospitals']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Hospital Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wongi', 'wongi')

        url = 'https://data.boston.gov/export/622/208/6222085d-ee88-45c6-ae40-0c7464620d64.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hospitals")
        repo.createCollection("hospitals")
        repo['wongi.hospitals'].insert_many(r)
        print('Load hospitals')
        for entry in repo.wongi.hospitals.find():
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
        repo.authenticate('wongi', 'wongi')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/622/208/')

        this_script = doc.agent('alg:wongi#hospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:6222085d-ee88-45c6-ae40-0c7464620d64', {'prov:label':'Boston Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hospitals, this_script)
        doc.usage(get_hospitals, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        hospitals = doc.entity('dat:wongi#hospitals', {prov.model.PROV_LABEL:'Boston Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospitals, this_script)
        doc.wasGeneratedBy(hospitals, get_hospitals, endTime)
        doc.wasDerivedFrom(hospitals, resource, get_hospitals, get_hospitals, get_hospitals)

        repo.logout()

        return doc
