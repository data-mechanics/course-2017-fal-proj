import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class Station_Network(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = []
    writes = ['yjunchoi_yzhang71.Station_Network']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        url = 'https://raw.githubusercontent.com/mbtaviz/mbtaviz.github.io/master/data/station-network.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Station_Network")
        repo.createCollection("Station_Network")

        """for key in r:
            delay = {}
            delay[key] = r[key]
            repo['yjunchoi_yzhang71.Station_Network'].insert(delay)"""

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

        this_script = doc.agent('alg:yjunchoi_yzhang71#Station_Network', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbtaviz:Station_Network', {'prov:label':'Station_Network', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Station_Network = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Station_Network, this_script)
        doc.usage(get_Station_Network, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        Station_Network = doc.entity('dat:yjunchoi_yzhang71#Station_Network', {prov.model.PROV_LABEL:'Station_Network in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Station_Network, this_script)
        doc.wasGeneratedBy(Station_Network, get_Station_Network, endTime)
        doc.wasDerivedFrom(Station_Network, resource, get_Station_Network, get_Station_Network, get_Station_Network)

        repo.logout()

        return doc

Station_Network.execute()
doc = Station_Network.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
