from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


class getPropertyValues(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.PropertyValues']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'https://data.cityofboston.gov/resource/n7za-nsjh.json'
        response = urlopen(url).read().decode("utf-8")

        r = json.loads(response)
        repo.dropCollection("PropertyValues")
        repo.createCollection("PropertyValues")
        repo['biel_otis.PropertyValues'].insert_many(r)
        repo['biel_otis.PropertyValues'].metadata({'complete':True})
        repo['biel_otis.PropertyValues'].metadata()
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
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        doc.add_namespace('alg', 'http://datamechanics.io/biel_otis/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/biel_otis/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/biel_otis/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/biel_otis/log/') # The event log.
        doc.add_namespace('property', 'https://data.cityofboston.gov/resource/') # Property values in city of Boston

        this_script = doc.agent('alg:biel_otis#getPropertyValues', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('property:n7za-nsjh', {'prov:label':'Property values in city of Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_property = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_property, this_script)
        
        doc.usage(get_property, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        prop = doc.entity('dat:biel_otis#prop', {prov.model.PROV_LABEL:'Property Values for the City of Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prop, this_script)
        doc.wasGeneratedBy(prop, get_property, endTime)
        doc.wasDerivedFrom(prop, resource, get_property, get_property, get_property)
        repo.logout()
          
        return doc

getPropertyValues.execute()
doc = getPropertyValues.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
