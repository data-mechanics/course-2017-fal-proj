import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from scipy.cluster.vq import kmeans2
import numpy as np

class optByMBTA(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = ['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation', 'cyyan_liuzirui_yjunchoi_yzhang71.MBTA_by_ward']
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
        MBTA = repo['cyyan_liuzirui_yjunchoi_yzhang71.MBTA_by_ward'].find({})

        repo.dropCollection("optByMBTA")
        repo.createCollection("optByMBTA")

        if trial == True:
            fWard = 10
            lWard = 14
        else:
            fWard = 1
            lWard = 23

        # Export Data from Dataset
        pLoc = {}
        for p in pLocation:
            if int(p['Ward']) < fWard and int(p['Ward']) >= lWard:
                continue
            else:
                pLoc[str(p['Ward'])] = p['coordinates']

        station = {}
        for m in MBTA:
            for i in range(fWard,lWard):
                station[str(i)] = m[str(i)]

        # Use k-mean Algorithm to optimize polling locations based on public transportation by wards
        optimized = {}
        for i in range(fWard,lWard):
            if station[str(i)] != []:
                pLoc[str(i)] = np.asarray(pLoc[str(i)])
                centroids, labels = kmeans2(station[str(i)], k = pLoc[str(i)], iter = 100, minit = 'matrix')
                optimized[str(i)] = centroids.tolist()
            else:
                optimized[str(i)] = pLoc[str(i)]

        results = [optimized]

        repo['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA'].insert(results)

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


        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#optByMBTA', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_pollingLocation = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#pollingLocation',
                                             {'prov:label': 'pollingLocation',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        resource_MBTA_by_ward = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#MBTA_by_ward',
                                             {'prov:label': 'MBTA_by_ward',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
                                              
        get_optByMBTA = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_optByMBTA, this_script)
        doc.usage(get_optByMBTA, resource_pollingLocation, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_optByMBTA, resource_MBTA_by_ward, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        optByMBTA = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#optByMBTA',
        {prov.model.PROV_LABEL:'Optimized Polling Location based on MBTA', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optByMBTA, this_script)
        doc.wasGeneratedBy(optByMBTA, get_optByMBTA, endTime)
        doc.wasDerivedFrom(optByMBTA, resource_pollingLocation, get_optByMBTA, get_optByMBTA, get_optByMBTA)
        doc.wasDerivedFrom(optByMBTA, resource_MBTA_by_ward, get_optByMBTA, get_optByMBTA, get_optByMBTA)

        repo.logout()

        return doc

# optByMBTA.execute()
# doc = optByMBTA.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
