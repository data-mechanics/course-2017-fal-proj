import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergeVotePopulation(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = ['yjunchoi_yzhang71.bostonPopulation', 'yjunchoi_yzhang71.presidentElectionByPrecinct']
    writes = ['yjunchoi_yzhang71.mergeVotePopulation']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        # loads the collection
        BP = repo['yjunchoi_yzhang71.bostonPopulation'].find()
        PE = repo['yjunchoi_yzhang71.presidentElectionByPrecinct'].find()

        mapBP = {}
        # Selection and Projection to calculate eligible voters estimates
        for row in BP:
            if row['Fact'] == 'Population estimates, July 1, 2016,  (V2016)':
                mapBP['Population estimates (2016)'] = row['Boston city, Massachusetts']
            if row['Fact'] == 'Persons under 18 years, percent, April 1, 2010':
                mapBP['Percentage under 18 (2010)'] = row['Boston city, Massachusetts']
        mapBP['Eligible voters estimates'] = int((float(selectionBP['Population estimates (2016)'][0:3]+selectionBP['Population estimates (2016)'][4:]) * (100 - float(selectionBP['Percentage under 18 (2010)'][0:4])))/100)
        eligibleVoters = mapBP['Eligible voters estimates']

        mapPE = {}
        # Select Boston Area and Total Votes from dataset
        for row in PE:
            if row['City/Town'] == 'Boston':
                mapPE['City/Town'] = row['City/Town']
                mapPE['Ward'] = row['Ward']
                mapPE['PCT'] = row['Pct']
                mapPE['Total Votes'] = row['Total Votes Cast']

        """
        repo.dropCollection("VoterByPopulation")
        repo.createCollection("VoterByPopulation")
        repo['yjunchoi_yzhang71.VoterByPopulation'].insert_many(voterByPopulation)
        repo['yjunchoi_yzhang71.VoterByPopulation'].metadata({'complete': True})
        print("Saved station_data", repo['yjunchoi_yzhang71.station_data'].metadata())
        """
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#mergeVotePopulation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_bostonPopulation = doc.entity('dat:yjunchoi_yzhang71#bostonPopulation',
                                             {'prov:label': 'bostonPopulation',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_presidentElectionByPrecinct = doc.entity('dat:yjunchoi_yzhang71#presidentElectionByPrecinct',
                                             {'prov:label': 'presidentElectionByPrecinct',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_voterByPopulation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_voterByPopulation, this_script)
        doc.usage(get_voterByPopulation, resource_bostonPopulation, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_voterByPopulation, resource_presidentElectionByPrecinct, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        voterByPopulation = doc.entity('dat:yjunchoi_yzhang71#voterByPopulation',
                          {prov.model.PROV_LABEL: 'voterByPopulation',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(voterByPopulation, this_script)
        doc.wasGeneratedBy(voterByPopulation, get_voterByPopulation, endTime)
        doc.wasDerivedFrom(voterByPopulation, resource_bostonPopulation, get_voterByPopulation, get_voterByPopulation, get_voterByPopulation)
        doc.wasDerivedFrom(voterByPopulation, resource_presidentElectionByPrecinct, get_voterByPopulation, get_voterByPopulation, get_voterByPopulation)

        repo.logout()

        return doc

mergeVotePopulation.execute()
doc = mergeVotePopulation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
