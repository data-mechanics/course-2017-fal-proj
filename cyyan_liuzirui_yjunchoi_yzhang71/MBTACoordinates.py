import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import yaml

class MBTACoordinates(dml.Algorithm):
    contributor = 'cyyan_liuzirui_yjunchoi_yzhang71'
    reads = []
    writes = ['cyyan_liuzirui_yjunchoi_yzhang71.MBTACoordinates']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

        url = 'http://erikdemaine.org/maps/mbta/mbta.yaml'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        mbta = yaml.load(response)

        repo.dropCollection("MBTACoordinates")
        repo.createCollection("MBTACoordinates")

        repo['cyyan_liuzirui_yjunchoi_yzhang71.MBTACoordinates'].insert_many(mbta)

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
        doc.add_namespace('eri', 'http://erikdemaine.org/maps/mbta')

        this_script = doc.agent('alg:cyyan_liuzirui_yjunchoi_yzhang71#MBTACoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('eri:mbta.yaml', {'prov:label':'MBTA Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'yaml'})
        get_MBTACoordinates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_MBTACoordinates, this_script)
        doc.usage(get_MBTACoordinates, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=MBTA+Coordinate&$select=MBTA, Coordinate'
                  }
                  )

        MBTACoordinates = doc.entity('dat:cyyan_liuzirui_yjunchoi_yzhang71#MBTACoordinates', {prov.model.PROV_LABEL:'MBTA Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(MBTACoordinates, this_script)
        doc.wasGeneratedBy(MBTACoordinates, get_MBTACoordinates, endTime)
        doc.wasDerivedFrom(MBTACoordinates, resource, get_MBTACoordinates, get_MBTACoordinates, get_MBTACoordinates)

        repo.logout()

        return doc
# 
# MBTACoordinates.execute()
# doc = MBTACoordinates.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
