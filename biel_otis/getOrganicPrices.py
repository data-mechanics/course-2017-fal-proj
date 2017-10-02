from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


class getOrganicPrices(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.OrganicPrices']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'http://datamechanics.io/data/biel_otis/food_prices.json'
        response = urlopen(url).read().decode("utf-8")
        #response = response.replace(']', '')
        #response = response.replace('[', '')
        #response = '[' + response + ']'

        r = json.loads(response)

        #s = json.dumps(r, sort_keys=True, indent=2)
        print(type(r))
        repo.dropCollection("OrganicPrices")
        repo.createCollection("OrganicPrices")
        repo['biel_otis.OrganicPrices'].insert_many(r)
        repo['biel_otis.OrganicPrices'].metadata({'complete':True})
        print(repo['biel_otis.OrganicPrices'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/biel_otis/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/biel_otis/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/biel_otis/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/biel_otis/log/') # The event log.
        doc.add_namespace('op', 'http://datamechanics.io/biel_otis/') # Organic Food Prices dataset in the United States

        this_script = doc.agent('alg:biel_otis#getOrganicPrices', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('op:food_prices', {'prov:label':'Organic Food Prices dataset in the United States', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_op = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_op, this_script)
        
        doc.usage(get_op, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        op = doc.entity('dat:biel_otis#op', {prov.model.PROV_LABEL:'Organic Food Prices dataset in the United States', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(op, this_script)
        doc.wasGeneratedBy(op, get_op, endTime)
        doc.wasDerivedFrom(op, resource, get_op, get_op, get_op)
        repo.logout()
        
        return doc

getOrganicPrices.execute()
doc = getOrganicPrices.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
