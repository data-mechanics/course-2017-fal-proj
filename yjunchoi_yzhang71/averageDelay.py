import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class averageDelay(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = []
    writes = ['yjunchoi_yzhang71.averageDelay']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        url = 'https://raw.githubusercontent.com/yzhang71/mbtaviz.github.io/master/data/average-actual-delays.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("averageDelay")
        repo.createCollection("averageDelay")

        for key in r:
            delay = {}
            delay[key] = r[key]
            repo['yjunchoi_yzhang71.averageDelay'].insert(delay)

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
        doc.add_namespace('mbtaviz', 'https://github.com/yzhang71/mbtaviz.github.io/blob/master/data/')

        this_script = doc.agent('alg:yjunchoi_yzhang71#averageDelay', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbtaviz:average-actual-delays', {'prov:label':'Average Actual Delay', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_averageDelay = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_averageDelay, this_script)
        doc.usage(get_averageDelay, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        averageDelay = doc.entity('dat:yjunchoi_yzhang71#averageDelay', {prov.model.PROV_LABEL:'Average Actual Delay of MBTA', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(averageDelay, this_script)
        doc.wasGeneratedBy(averageDelay, get_averageDelay, endTime)
        doc.wasDerivedFrom(averageDelay, resource, get_averageDelay, get_averageDelay, get_averageDelay)

        repo.logout()

        return doc

averageDelay.execute()
doc = averageDelay.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
