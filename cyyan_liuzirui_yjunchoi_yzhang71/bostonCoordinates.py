import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geoql

class bostonCoordinates(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = []
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.bostonCoordinates']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/142500a77e2a4dbeb94a86f7e0b568bc_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bostonCoordinates")
        repo.createCollection("bostonCoordinates")

        #TODO: Find the way to use geoql and check mongodb
        for key in r:
            delay = {}
            delay[key] = r[key]
            repo['cyyan_liuzirui_yjunchoi_yzhang71.bostonCoordinates'].insert(delay)

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
        doc.add_namespace('bod', 'http://bostonpoendata-boston.opendata.argcis.com/datasets/')

        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#bostonCoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:142500a77e2a4dbeb94a86f7e0b568bc_0.geojson', {'prov:label':'Boston Boundary', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_bostonCoordinates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bostonCoordinates, this_script)
        doc.usage(get_bostonCoordinates, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Boston+Boundary&$select=Boston, Polygon'
                  }
                  )

        bostonCoordinates = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#bostonCoordinates', {prov.model.PROV_LABEL:'Boston Boundary Coordinate', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonCoordinates, this_script)
        doc.wasGeneratedBy(bostonCoordinates, get_bostonCoordinates, endTime)
        doc.wasDerivedFrom(bostonCoordinates, resource, get_bostonCoordinates, get_bostonCoordinates, get_bostonCoordinates)

        repo.logout()

        return doc

# bostonCoordinates.execute()
# doc = bostonCoordinates.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
