from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


class getObesityData(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.Obesity']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'https://chronicdata.cdc.gov/resource/ahrt-wk9b.json?$offset=13908&$limit=5000'
        response = urlopen(url).read().decode("utf-8")
        #response = response.replace(']', '')
        #response = response.replace('[', '')
        #response = '[' + response + ']'

        r = json.loads(response)

        #s = json.dumps(r, sort_keys=True, indent=2)
        print(type(r))
        repo.dropCollection("ObesityData")
        repo.createCollection("ObesityData")
        repo['biel_otis.ObesityData'].insert_many(r)
        repo['biel_otis.ObesityData'].metadata({'complete':True})
        print(repo['biel_otis.ObesityData'].metadata())

        """
        url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("found")
        repo.createCollection("found")
        repo['biel_otis.found'].insert_many(r)
        """
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
        repo.authenticate('biel_otis', 'biel_otis')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('obe', 'https://chronicdata.cdc.gov/resource/') # Obesity dataset from chronicdata.cdc

        this_script = doc.agent('alg:biel_otis#getObesityData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('obe:4ahrt-wk9b.json?$offset=13908&$limit=5000', {'prov:label':'Obesity Data for inhabitants of Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_obesity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_obesity, this_script)
        
        doc.usage(get_obesity, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        obesity = doc.entity('dat:biel_otis#obesity', {prov.model.PROV_LABEL:'Obesity Data for inhabitants of Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(obesity, this_script)
        doc.wasGeneratedBy(obesity, get_obesity, endTime)
        doc.wasDerivedFrom(obesity, resource, get_obesity, get_obesity, get_obesity)
        repo.logout()
        
        return doc

getObesityData.execute()
doc = getObesityData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
