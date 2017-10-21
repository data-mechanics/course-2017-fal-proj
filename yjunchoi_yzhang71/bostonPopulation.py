import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pandas as pd


class bostonPopulation(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = []
    writes = ['yjunchoi_yzhang71.bostonPopulation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        repo.dropCollection("bostonPopulation")
        repo.createCollection("bostonPopulation")

        url = 'http://datamechanics.io/data/yjunchoi_yzhang71/BostonPopulation.csv'
        #Originally from 'https://www.census.gov/quickfacts/fact/map/brooklinecdpmassachusetts,bostoncitymassachusetts/PST045216#viewtop'
        #Code for csv read with sth
        urllib.request.urlretrieve(url, 'population.csv')
        vote_df = pd.read_csv('population.csv')
        repo['yjunchoi_yzhang71.bostonPopulation'].insert_many(vote_df.to_dict('records'))
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
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/yjunchoi_yzhang71') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/yjunchoi_yzhang71') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:yjunchoi_yzhang71#bostonPopulation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:BostonPopulation.csv', {'prov:label':'Boston Population Estimates 2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_bostonPopulation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bostonPopulation, this_script)
        doc.usage(get_bostonPopulation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Boston+Population&$select=Fact,Boston'
                  }
                  )

        bostonPopulation = doc.entity('dat:yjunchoi_yzhang71#bostonPopulation', {prov.model.PROV_LABEL:'Boston Population', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonPopulation, this_script)
        doc.wasGeneratedBy(bostonPopulation, get_bostonPopulation, endTime)
        doc.wasDerivedFrom(bostonPopulation, resource, get_bostonPopulation, get_bostonPopulation, get_bostonPopulation)

        repo.logout()

        return doc

#bostonPopulation.execute()
#doc = bostonPopulation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
