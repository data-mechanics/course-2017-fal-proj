import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import random
import math

class scoringLocation(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = ['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation', 'cyyan_liuzirui_yjunchoi_yzhang71.optByBusstop', 'cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA', 'cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT']
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.scoringLocation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
        optByBusstop = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByBusstop'].find()
        optByMBTA = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA'].find()
        optByPublicT = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT'].find()

        repo.dropCollection("scoringLocation")
        repo.createCollection("scoringLocation")

        # Export Data from Dataset
        pLoc = []
        for p in pLocation:
            pLoc.append(p['coordinates'])

        bStop = []
        for b in optByBusstop:
            for i in range(1,23):
                bStop.append(b[str(i)])

        station = []
        for m in optByMBTA:
            for i in range(1,23):
                station.append(m[str(i)])

        publicT = []
        for pb in optByPublicT:
            for i in range(1,23):
                publicT.append(pb[str(i)])

        # Randomize the coordinates of voter's address
        voterCoordinates = []
        for i in range(10000):
            lat = 0.09 # max latitude - min latitude in Boston City
            lng = 0.12 # max longitude - min longitude in Boston City
            lat *= random.uniform(0,1)
            lat += 42.23
            lng *= random.uniform(0,1)
            lng += 71.08
            voterCoordinates.append([lng,lat])


        #repo['cyyan_liuzirui_yjunchoi_yzhang71.scoringLocation'].insert(results)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def calcuateScore(voter, polling):
        # TODO: Create a method to calculate Euclidean distances.
        result = []
        for i in range(len(voter)):
            score = []
            for j in range(len(polling)):
                distance = abs((polling[j][0] - voter[i][0]) ** 2 + (polling[j][1] - voter[i][1]) ** 2)
                score.append(distance)
            result.append(math.sqrt(min(score)))
        return result

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


        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#scoringLocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_pollingLocation = doc.entity('dat:yjunchoi_yzhang71#pollingLocation',
                                             {'prov:label': 'pollingLocation',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_bus_by_ward = doc.entity('dat:yjunchoi_yzhang71#bus_by_ward',
                                             {'prov:label': 'bus_by_ward',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        resource_MBTA_by_ward = doc.entity('dat:yjunchoi_yzhang71#MBTA_by_ward',
                                             {'prov:label': 'MBTA_by_ward',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        get_scoringLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_scoringLocation, this_script)
        doc.usage(get_scoringLocation, resource_pollingLocation, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_scoringLocation, resource_bus_by_ward, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_scoringLocation, resource_MBTA_by_ward, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        scoringLocation = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#scoringLocation',
        {prov.model.PROV_LABEL:'Optimized Polling Location based on Public Transportation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(scoringLocation, this_script)
        doc.wasGeneratedBy(scoringLocation, get_scoringLocation, endTime)
        doc.wasDerivedFrom(scoringLocation, resource_MBTA_by_ward, get_scoringLocation, get_scoringLocation, get_scoringLocation)
        doc.wasDerivedFrom(scoringLocation, resource_bus_by_ward, get_scoringLocation, get_scoringLocation, get_scoringLocation)
        doc.wasDerivedFrom(scoringLocation, resource_pollingLocation, get_scoringLocation, get_scoringLocation, get_scoringLocation)

        repo.logout()

        return doc

# scoringLocation.execute()
# doc = scoringLocation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
#
# # eof
