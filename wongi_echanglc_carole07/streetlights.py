import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class streetlights(dml.Algorithm):
    contributor = 'wongi'
    reads = []
    writes = ['wongi.streetlights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Streetlights Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wongi', 'wongi')

        url = 'https://data.boston.gov/datastore/odata3.0/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5?$top=500&$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("streetlights")
        repo.createCollection("streetlights")
        repo['wongi.streetlights'].insert_many(r['value'])
        print('Load streetlights')
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def execute(trial = True):
        '''Retrieve Boston Streetlights Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wongi', 'wongi')
        
        url = 'https://data.boston.gov/datastore/odata3.0/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5?$top=10&$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("streetlights")
        repo.createCollection("streetlights")
        repo['wongi.streetlights'].insert_many(r['value'])
        print('Load streetlights')
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
        doc.add_namespace('bdp', 'https://data.boston.gov/datastore/odata3.0/')

        this_script = doc.agent('alg:wongi#streetlights', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5', {'prov:label':'Boston Streetlights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.usage(get_streetlights, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        streetlights = doc.entity('dat:wongi#streetlights', {prov.model.PROV_LABEL:'Boston Streetlights ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetlights, this_script)
        doc.wasGeneratedBy(streetlights, get_streetlights, endTime)
        doc.wasDerivedFrom(streetlights, resource, get_streetlights, get_streetlights, get_streetlights)

        repo.logout()

        return doc
