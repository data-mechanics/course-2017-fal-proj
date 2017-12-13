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
    def calculateScore(voter, polling):
        result = []
        for i in range(len(voter)):
            score = []
            for j in range(len(polling)):
                distance = abs((polling[j][0] - voter[i][0]) ** 2 + (polling[j][1] - voter[i][1]) ** 2)
                score.append(distance)
            result.append(math.sqrt(min(score)))
        return result

    @staticmethod
    def calculateStatistics(lst):
        average = scoringLocation.avg(lst)
        std = scoringLocation.stddev(lst)
        low, high = scoringLocation.confidenceInterval(lst, 0.95)
        dic = {}
        dic['avg'] = average
        dic['stddev'] = std
        dic['lowCI95'] = low
        dic['highCI95'] = high
        return dic

    # Helper codes from lecture notes
    @staticmethod
    def avg(x): # Average
        return sum(x)/len(x)

    @staticmethod
    def stddev(x): # Standard deviation.
        m = scoringLocation.avg(x)
        return math.sqrt(sum([(xi-m)**2 for xi in x])/len(x))

    @staticmethod
    def confidenceInterval(lst, ci = 0.95):
        alpha = (1-ci)/2
        low = int(alpha * len(lst))
        high = int((1-alpha) * len(lst))
        lst.sort()
        return lst[low], lst[high]

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

        # Trial mode
        if trial == True:
            sample = 100
            fWard = 10
            lWard = 14
        else:
            sample = 10000
            fWard = 1
            lWard = 23

        # Export Data from Dataset
        pLoc = []
        for p in pLocation:
            if int(p['Ward']) < fWard and int(p['Ward']) >= lWard:
                continue
            else:
                pLoc += p['coordinates']

        bStop = []
        for b in optByBusstop:
            for i in range(fWard,lWard):
                bStop += b[str(i)]

        station = []
        for m in optByMBTA:
            for i in range(fWard,lWard):
                station += m[str(i)]

        publicT = []
        for pb in optByPublicT:
            for i in range(fWard,lWard):
                publicT += pb[str(i)]

        # Randomize the coordinates of voter's address (Sampling)
        voterCoordinates = []
        for i in range(sample):
            lat = 0.09 # max latitude - min latitude in Boston City
            lng = 0.12 # max longitude - min longitude in Boston City
            lat *= random.uniform(0,1)
            lat += 42.23
            lng *= random.uniform(0,1)
            lng += 71.08
            lng *= -1
            voterCoordinates.append([lng,lat])

        # Calculate scores from polling locations from each optimzed result
        oLoc = scoringLocation.calculateScore(voterCoordinates, pLoc)
        bLoc = scoringLocation.calculateScore(voterCoordinates, bStop)
        mLoc = scoringLocation.calculateScore(voterCoordinates, station)
        pbLoc = scoringLocation.calculateScore(voterCoordinates, publicT)

        # Calculate average, standard deviation (95% CI)
        result = {}
        result['pollingLocation'] = scoringLocation.calculateStatistics(oLoc)
        result['optByBusstop'] = scoringLocation.calculateStatistics(bLoc)
        result['optByMBTA'] = scoringLocation.calculateStatistics(mLoc)
        result['optByPublicT'] = scoringLocation.calculateStatistics(pbLoc)

        results = [result]
        repo['cyyan_liuzirui_yjunchoi_yzhang71.scoringLocation'].insert(results)

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


        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#scoringLocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_pollingLocation = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#pollingLocation',
                                             {'prov:label': 'pollingLocation',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        resource_optByBusstop = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#optByBusstop',
                                             {'prov:label': 'Optimization by bus stop',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        resource_optByMBTA = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#optByMBTA',
                                             {'prov:label': 'Optimization by MBTA',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        resource_optByPublicT = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#optByPublicT',
                                             {'prov:label': 'Optimization by public transportation',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_scoringLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_scoringLocation, this_script)
        doc.usage(get_scoringLocation, resource_pollingLocation, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        doc.usage(get_scoringLocation, resource_optByBusstop, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        doc.usage(get_scoringLocation, resource_optByMBTA, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        doc.usage(get_scoringLocation, resource_optByPublicT, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        scoringLocation = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#scoringLocation',
        {prov.model.PROV_LABEL:'Optimized Polling Location based on Public Transportation', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(scoringLocation, this_script)
        doc.wasGeneratedBy(scoringLocation, get_scoringLocation, endTime)
        doc.wasDerivedFrom(scoringLocation, resource_optByPublicT, get_scoringLocation, get_scoringLocation, get_scoringLocation)
        doc.wasDerivedFrom(scoringLocation, resource_optByMBTA, get_scoringLocation, get_scoringLocation, get_scoringLocation)
        doc.wasDerivedFrom(scoringLocation, resource_optByBusstop, get_scoringLocation, get_scoringLocation, get_scoringLocation)
        doc.wasDerivedFrom(scoringLocation, resource_pollingLocation, get_scoringLocation, get_scoringLocation, get_scoringLocation)

        repo.logout()

        return doc

# scoringLocation.execute()
# doc = scoringLocation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
