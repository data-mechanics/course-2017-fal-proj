import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import yaml

class MBTACoordinates(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = []
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.MBTACoordinates']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')

        url = 'http://erikdemaine.org/maps/mbta/mbta.yaml'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        mbta = yaml.load(response)

        repo.dropCollection("MBTACoordinates")
        repo.createCollection("MBTACoordinates")

        repo['yjunchoi_yzhang71_cyyan_liuzirui.MBTACoordinates'].insert_many(mbta)

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
        doc.add_namespace('eri', 'http://erikdemaine.org/maps/mbta')

        this_script = doc.agent('alg:yjunchoi_yzhang71_cyyan_liuzirui#MBTACoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('eri:mbta.yaml', {'prov:label':'MBTA Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_MBTACoordinates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_MBTACoordinates, this_script)
        doc.usage(get_MBTACoordinates, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Station+Coordinate&$select=Station, Coordinate'
                  }
                  )

        MBTACoordinates = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#MBTACoordinates', {prov.model.PROV_LABEL:'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(MBTACoordinates, this_script)
        doc.wasGeneratedBy(MBTACoordinates, get_MBTACoordinates, endTime)
        doc.wasDerivedFrom(MBTACoordinates, resource, get_MBTACoordinates, get_MBTACoordinates, get_MBTACoordinates)

        repo.logout()

        return doc

MBTACoordinates.execute()
doc = MBTACoordinates.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
