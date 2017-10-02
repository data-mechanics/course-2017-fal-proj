import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pandas as pd


class presidentElectionByPrecinct(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = []
    writes = ['yjunchoi_yzhang71.presidentElectionByPrecinct']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        repo.dropCollection("presidentElectionByPrecinct")
        repo.createCollection("presidentElectionByPrecinct")

        url = 'https://raw.githubusercontent.com/WHYjun/course-2017-fal-proj/master/local_data/presidentElectionByPrecinct.csv'
        #Originally from 'http://electionstats.state.ma.us/elections/download/40060/precincts_include:1/'
        # Code for csv read with sth
        urllib.request.urlretrieve(url, 'vote.csv')
        vote_df = pd.read_csv('vote.csv')
        repo['yjunchoi_yzhang71.presidentElectionByPrecinct'].insert_many(vote_df.to_dict('records'))
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
        doc.add_namespace('mes', 'http://electionstats.state.ma.us/elections/download/40060/') # Massachusetts Election Statistics

        this_script = doc.agent('alg:yjunchoi_yzhang71#presidentElectionByPrecinct', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mes:precincts_include:1', {'prov:label':'President General Election by Prencinct', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_presidentElection = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_presidentElection, this_script)
        doc.usage(get_presidentElection, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=President+Election&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        presidentElectionByPrecinct = doc.entity('dat:yjunchoi_yzhang71#presidentElectionByPrecinct', {prov.model.PROV_LABEL:'Presidet Election by Precinct', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(presidentElectionByPrecinct, this_script)
        doc.wasGeneratedBy(presidentElectionByPrecinct, get_presidentElection, endTime)
        doc.wasDerivedFrom(presidentElectionByPrecinct, resource, get_presidentElection, get_presidentElection, get_presidentElection)

        repo.logout()

        return doc

presidentElectionByPrecinct.execute()
doc = presidentElectionByPrecinct.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
