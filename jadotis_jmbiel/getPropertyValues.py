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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('property', 'https://data.cityofboston.gov/resource/') # Property values in city of Boston

        this_script = doc.agent('alg:biel_otis#getPropertyValues', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('property:n7za-nsjh', {'prov:label':'Property values in city of Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        output_resource = doc.entity('dat:biel_otis#PropertyValues', {prov.model.PROV_LABEL: 'Dataset containing property values around the city of Boston.', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    
        
        #Associations
        doc.wasAssociatedWith(this_run, this_script)
     
        #Usages
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        #Generated
        doc.wasGeneratedBy(output_resource, this_run, endTime)


        #Attributions
        doc.wasAttributedTo(output_resource, this_script)

        #Derivations
        doc.wasDerivedFrom(output_resource, resource, this_run, this_run, this_run)
        repo.logout()
          
        return doc

print("Finished getPropertyValues")

## eof
