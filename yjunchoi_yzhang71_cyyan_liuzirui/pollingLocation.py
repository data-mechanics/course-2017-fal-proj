import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pandas as pd


class pollingLocation(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = []
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.pollingLocation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')

        repo.dropCollection("pollingLocation")
        repo.createCollection("pollingLocation")

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/053b0359485d435abfb525e07e298885_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        raw = json.loads(response)
        s = json.dumps(raw, sort_keys = True, indent = 2)

        Ward_coordinates = {}
        for i in range(1,23):
            coordinate=[]
            for j in raw['features']:
                if i == j['properties']['Ward']:
                    coordinate.append(j['geometry']['coordinates'])
                    
            Ward_coordinates[i] = coordinate

        results = [ {'Ward': key,  'coordinates': Ward_coordinates[key]}  for key in Ward_coordinates ]


        repo['yjunchoi_yzhang71_cyyan_liuzirui.pollingLocation'].insert_many(results)
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
        doc.add_namespace('bod', 'http://bostonpoendata-boston.opendata.argcis.com/datasets/') # Dataset used

        this_script = doc.agent('alg:yjunchoi_yzhang71_cyyan_liuzirui#pollingLocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:053b0359485d435abfb525e07e298885_0.geojson', {'prov:label':'Polling Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_pollingLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_pollingLocation, this_script)
        doc.usage(get_pollingLocation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        pollingLocation = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#pollingLocation', {prov.model.PROV_LABEL:'Polling Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pollingLocation, this_script)
        doc.wasGeneratedBy(pollingLocation, get_pollingLocation, endTime)
        doc.wasDerivedFrom(pollingLocation, resource, get_pollingLocation, get_pollingLocation, get_pollingLocation)

        repo.logout()

        return doc

pollingLocation.execute()
doc = pollingLocation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
