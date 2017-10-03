from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


class getWinterMarkets(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.WinterFarmersMarkets']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'https://data.cityofboston.gov/resource/txud-qumr.json'
        response = urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        repo.dropCollection("WinterFarmersMarkets")
        repo.createCollection("WinterFarmersMarkets")
        repo['biel_otis.WinterFarmersMarkets'].insert_many(r)
        repo['biel_otis.WinterFarmersMarkets'].metadata({'complete':True})
        print(repo['biel_otis.WinterFarmersMarkets'].metadata())
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
        doc.add_namespace('wfm', 'https://data.cityofboston.gov/resource/') # Dataset of all Winter Farmers Markets in the city of Boston

        this_script = doc.agent('alg:biel_otis#getWinterMarkets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('wfm:txud-qumr.json', {'prov:label':'Data of all Winter Farmers Markets in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_wfm = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_wfm, this_script)
        
        doc.usage(get_wfm, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        wfm = doc.entity('dat:biel_otis#wfm', {prov.model.PROV_LABEL:'Data of all Winter Farmers Markets in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasGeneratedBy(wfm, get_wfm, endTime)
        doc.wasDerivedFrom(wfm, resource, get_wfm, get_wfm, get_wfm)
        repo.logout()
    
        return doc

getWinterMarkets.execute()
doc = getWinterMarkets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
