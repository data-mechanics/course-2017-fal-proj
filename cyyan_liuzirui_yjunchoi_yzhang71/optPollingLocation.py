import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pandas as pd


class optPollingLocation(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = ['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation', 'cyyan_liuzirui_yjunchoi_yzhang71.busstopCoordinates', 'cyyan_liuzirui_yjunchoi_yzhang71.MBTACoordinates']
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.optPollingLocation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
        busstop = repo['cyyan_liuzirui_yjunchoi_yzhang71.busstopCoordinates'].find()
        MBTA = repo['cyyan_liuzirui_yjunchoi_yzhang71.MBTACoordinates'].find({})

        repo.dropCollection("optPollingLocation")
        repo.createCollection("optPollingLocation")

        # Adjusting polling locations in Pandas
        pLoc = pd.DataFrame(list(pLocation))
        pLoc['coordinates'] = list(pLoc.coordinates)

        # Adjusting bus stops in Pandas
        bStop = pd.DataFrame(list(busstop))
        bStop['coordinates'] = list(bStop.coordinates)


        dfMBTA = pd.DataFrame(list(MBTA))
        dfMBTA['coordinates'] = list(dfMBTA.)
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
        doc.add_namespace('dat', 'http://datamechanics.io/data/cyyan_liuzirui_yjunchoi_yzhang71') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonpoendata-boston.opendata.argcis.com/datasets/') # Dataset used

        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#optPollingLocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:f7c6dc9eb6b14463a3dd87451beba13f_5.csv', {'prov:label':'Polling Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_optPollingLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_optPollingLocation, this_script)
        doc.usage(get_optPollingLocation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        optPollingLocation = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#optPollingLocation', {prov.model.PROV_LABEL:'Polling Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optPollingLocation, this_script)
        doc.wasGeneratedBy(optPollingLocation, get_optPollingLocation, endTime)
        doc.wasDerivedFrom(optPollingLocation, resource, get_optPollingLocation, get_optPollingLocation, get_optPollingLocation)

        repo.logout()

        return doc

# optPollingLocation.execute()
# doc = optPollingLocation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
