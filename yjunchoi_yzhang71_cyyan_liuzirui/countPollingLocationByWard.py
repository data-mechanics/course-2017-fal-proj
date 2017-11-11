import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class countPollingLocationByWard(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = ['yjunchoi_yzhang71_cyyan_liuzirui.pollingLocation', 'yjunchoi_yzhang71_cyyan_liuzirui.presidentElectionByPrecinct']
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.countPollingLocationByWard']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')

        # loads the collection
        PL = repo['yjunchoi_yzhang71_cyyan_liuzirui.pollingLocation'].find()
        PE = repo['yjunchoi_yzhang71_cyyan_liuzirui.presidentElectionByPrecinct'].find()

        mapPEList = []
        voterTotal = 0
        # Select Boston Area and Total Votes from dataset
        for row in PE:
            if row['City/Town'] == 'Boston':
                mapPEList.append({'City/Town':row['City/Town'], 'Ward':row['Ward']})

        # Group by Ward and count the number of polling location
        wardList = [] # 22 wards in Boston
        for row in PL:
        
            wardList.append(len(row['coordinates'])) # index starts from 0
        print(wardList)

        countLocation = []
        for i in range(22):
            countLocation.append({"Ward"+str(i+1):wardList[i]})


        repo.dropCollection("countPollingLocationByWard")
        repo.createCollection("countPollingLocationByWard")
        repo['yjunchoi_yzhang71_cyyan_liuzirui.countPollingLocationByWard'].insert(countLocation)
        repo['yjunchoi_yzhang71_cyyan_liuzirui.countPollingLocationByWard'].metadata({'complete': True})
        print("Saved countPollingLocationByWard", repo['yjunchoi_yzhang71_cyyan_liuzirui.countPollingLocationByWard'].metadata())

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
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#countPollingLocationByWard',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_presidentElectionByPrecinct = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#presidentElectionByPrecinct',
                                             {'prov:label': 'presidentElectionByPrecinct',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_pollingLocation = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#pollingLocation',
                                             {'prov:label': 'pollingLocation',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_countPollingLocationByWard = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_countPollingLocationByWard, this_script)
        doc.usage(get_countPollingLocationByWard, resource_presidentElectionByPrecinct, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=president+elction+by+precinct$select=city/town, ward'})
        doc.usage(get_countPollingLocationByWard, resource_pollingLocation, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        countPollingLocationByWard = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#countPollingLocationByWard',
                          {prov.model.PROV_LABEL: 'countPollingLocationByWard',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(countPollingLocationByWard, this_script)
        doc.wasGeneratedBy(countPollingLocationByWard, get_countPollingLocationByWard, endTime)
        doc.wasDerivedFrom(countPollingLocationByWard, resource_presidentElectionByPrecinct, get_countPollingLocationByWard, get_countPollingLocationByWard, get_countPollingLocationByWard)
        doc.wasDerivedFrom(countPollingLocationByWard, resource_pollingLocation, get_countPollingLocationByWard, get_countPollingLocationByWard, get_countPollingLocationByWard)

        repo.logout()

        return doc

countPollingLocationByWard.execute()
doc = countPollingLocationByWard.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
