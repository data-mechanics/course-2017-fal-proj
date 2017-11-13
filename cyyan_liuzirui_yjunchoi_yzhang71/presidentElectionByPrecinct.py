import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pandas as pd


class presidentElectionByPrecinct(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = []
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.presidentElectionByPrecinct']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        repo.dropCollection("presidentElectionByPrecinct")
        repo.createCollection("presidentElectionByPrecinct")

        url = 'http://datamechanics.io/data/yjunchoi_yzhang71/presidentElectionByPrecinct.csv'
        # Code for csv read with sth
        urllib.request.urlretrieve(url, 'vote.csv')
        vote_df = pd.read_csv('vote.csv')
        repo['cyyan_liuzirui_yjunchoi_yzhang71.presidentElectionByPrecinct'].insert_many(vote_df.to_dict('records'))
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
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/cyyan_liuzirui_yjunchoi_yzhang71') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/yjunchoi_yzhang71') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#presidentElectionByPrecinct', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:presidentElectionByPrecinct.csv', {'prov:label':'President General Election by Prencinct', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_presidentElection = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_presidentElection, this_script)
        doc.usage(get_presidentElection, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        presidentElectionByPrecinct = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#presidentElectionByPrecinct', {prov.model.PROV_LABEL:'Presidet Election by Precinct', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(presidentElectionByPrecinct, this_script)
        doc.wasGeneratedBy(presidentElectionByPrecinct, get_presidentElection, endTime)
        doc.wasDerivedFrom(presidentElectionByPrecinct, resource, get_presidentElection, get_presidentElection, get_presidentElection)

        repo.logout()

        return doc

# presidentElectionByPrecinct.execute()
# doc = presidentElectionByPrecinct.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
