import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class busstopCoordinates(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = []
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.busstopCoordinates']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')

        url = 'http://datamechanics.io/data/yjunchoi_yzhang71/coordinate.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("busstopCoordinates")
        repo.createCollection("busstopCoordinates")

        for key in r:
            delay = {}
            delay[key] = r[key]
            repo['yjunchoi_yzhang71_cyyan_liuzirui.busstopCoordinates'].insert(delay)

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
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/yjunchoi_yzhang71_cyyan_liuzirui') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/yjunchoi_yzhang71_cyyan_liuzirui') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dat', 'http://datamechanics.io/data/yjunchoi_yzhang71/') #MBTA Data Set

        this_script = doc.agent('alg:yjunchoi_yzhang71_cyyan_liuzirui#busstopCoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:coordinate.json', {'prov:label':'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_busstopCoordinates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_busstopCoordinates, this_script)
        doc.usage(get_busstopCoordinates, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Station+Coordinate&$select=Station, Coordinate'
                  }
                  )

        busstopCoordinates = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#busstopCoordinates', {prov.model.PROV_LABEL:'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(busstopCoordinates, this_script)
        doc.wasGeneratedBy(busstopCoordinates, get_busstopCoordinates, endTime)
        doc.wasDerivedFrom(busstopCoordinates, resource, get_busstopCoordinates, get_busstopCoordinates, get_busstopCoordinates)

        repo.logout()

        return doc

busstopCoordinates.execute()
doc = busstopCoordinates.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
