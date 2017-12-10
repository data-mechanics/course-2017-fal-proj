import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class busstopCoordinates(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = []
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.busstopCoordinates']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        url = 'http://datamechanics.io/data/wuhaoyu_yiran123/MBTA_Bus_Stops.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        raw = json.loads(response)
        s = json.dumps(raw, sort_keys=True, indent=2)
        repo.dropCollection("busstopCoordinates")
        repo.createCollection("busstopCoordinates")

        coordinates = {}
        for i in raw['features']:
            coordinates[i['properties']['STOP_NAME']] = i['geometry']['coordinates']

        results = [ {'name': key,  'coordinates': coordinates[key]}  for key in coordinates ]

        repo['cyyan_liuzirui_yjunchoi_yzhang71.busstopCoordinates'].insert_many(results)

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
        doc.add_namespace('oth', 'http://datamechanics.io/data/wuhaoyu_yiran123') #Data Source from the other team

        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#busstopCoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('oth:MBTA_Bus_Stops.geojson', {'prov:label':'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_busstopCoordinates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_busstopCoordinates, this_script)
        doc.usage(get_busstopCoordinates, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Busstop+Coordinate&$select=Busstop, Coordinate'
                  }
                  )

        busstopCoordinates = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#busstopCoordinates', {prov.model.PROV_LABEL:'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(busstopCoordinates, this_script)
        doc.wasGeneratedBy(busstopCoordinates, get_busstopCoordinates, endTime)
        doc.wasDerivedFrom(busstopCoordinates, resource, get_busstopCoordinates, get_busstopCoordinates, get_busstopCoordinates)

        repo.logout()

        return doc

# busstopCoordinates.execute()
# doc = busstopCoordinates.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
