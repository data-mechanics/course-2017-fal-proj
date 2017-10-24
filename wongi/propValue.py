import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class propValue(dml.Algorithm):
    contributor = 'wongi'
    reads = []
    writes = ['wongi.propValue']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Property Value Data from Analyze Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wongi', 'wongi')

        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("propValue")
        repo.createCollection("propValue")
        repo['wongi.propValue'].insert_many(r)
        print('Load property value')
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:wongi#propValue', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Boston Property Values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_propValue = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_propValue, this_script)
        doc.usage(get_propValue, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        propValue = doc.entity('dat:wongi#propValue', {prov.model.PROV_LABEL:'Boston Property Values ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(propValue, this_script)
        doc.wasGeneratedBy(propValue, get_propValue, endTime)
        doc.wasDerivedFrom(propValue, resource, get_propValue, get_propValue, get_propValue)

        repo.logout()

        return doc
