import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from scipy.cluster.vq import kmeans2
import numpy as np

class optByPublicT(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = ['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation', 'cyyan_liuzirui_yjunchoi_yzhang71.bus_by_ward', 'cyyan_liuzirui_yjunchoi_yzhang71.MBTA_by_ward']
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
        busstop = repo['cyyan_liuzirui_yjunchoi_yzhang71.bus_by_ward'].find()
        MBTA = repo['cyyan_liuzirui_yjunchoi_yzhang71.MBTA_by_ward'].find()

        repo.dropCollection("optByPublicT")
        repo.createCollection("optByPublicT")

        # Export Data from Dataset
        pLoc = {}
        for p in pLocation:
            pLoc[str(p['Ward'])] = p['coordinates']

        bStop = {}
        for b in busstop:
            for i in range(1,23):
                bStop[str(i)] = b[str(i)]

        station = {}
        for m in MBTA:
            for i in range(1,23):
                station[str(i)] = m[str(i)]


        # Combine the coordinates of public transportation
        publicT = {}
        for i in range(1,23):
            coordinates = []
            coordinates.extend(station[str(i)])
            coordinates.extend(bStop[str(i)])
            publicT[str(i)] = coordinates


        # Use k-mean Algorithm to optimize polling locations based on public transportation by wards
        optimized = {}
        for i in range(1,23):
            pLoc[str(i)] = np.asarray(pLoc[str(i)])
            centroids, labels = kmeans2(publicT[str(i)], k = pLoc[str(i)], iter = 100, minit = 'matrix')
            optimized[str(i)] = centroids.tolist()

        results = [optimized]

        repo['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT'].insert(results)

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
        doc.add_namespace('dat', 'http://datamechanics.io/data/cyyan_liuzirui_yjunchoi_yzhang71') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonpoendata-boston.opendata.argcis.com/datasets/') # Dataset used

        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#optByPublicT', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:f7c6dc9eb6b14463a3dd87451beba13f_5.csv', {'prov:label':'Polling Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_optByPublicT = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_optByPublicT, this_script)
        doc.usage(get_optByPublicT, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        optByPublicT = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#optByPublicT', {prov.model.PROV_LABEL:'Polling Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optByPublicT, this_script)
        doc.wasGeneratedBy(optByPublicT, get_optByPublicT, endTime)
        doc.wasDerivedFrom(optByPublicT, resource, get_optByPublicT, get_optByPublicT, get_optByPublicT)

        repo.logout()

        return doc

optByPublicT.execute()
doc = optByPublicT.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
