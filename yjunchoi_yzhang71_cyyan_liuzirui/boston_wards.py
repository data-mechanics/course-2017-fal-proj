import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class boston_wards(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = []
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.boston_wards']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui', 'yjunchoi_yzhang71_cyyan_liuzirui')

        url = 'http://datamechanics.io/data/yjunchoi_yzhang71/Wards.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        raw = json.loads(response)
        s = json.dumps(raw, sort_keys=True, indent=2)
        repo.dropCollection("wards")
        repo.createCollection("wards")

        coordinates = {}
        for i in raw['features']:
            coordinates[i['properties']['WARD']] = i['geometry']['coordinates']

        results = [ {'ward_num': key,  'coordinates': coordinates[key][0]}  for key in coordinates ]

        repo['yjunchoi_yzhang71_cyyan_liuzirui.wards'].insert_many(results)

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
        doc.add_namespace('oth', 'http://datamechanics.io/data/wuhaoyu_yiran123') #Data Source from the other team

        this_script = doc.agent('alg:yjunchoi_yzhang71_cyyan_liuzirui#boston_wards', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('oth:MBTA_Bus_Stops.geojson', {'prov:label':'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_wards = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_wards, this_script)
        doc.usage(get_boston_wards, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Busstop+Coordinate&$select=Busstop, Coordinate'
                  }
                  )

        boston_wards = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#boston_wards', {prov.model.PROV_LABEL:'Bus Stop Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_wards, this_script)
        doc.wasGeneratedBy(boston_wards, get_boston_wards, endTime)
        doc.wasDerivedFrom(boston_wards, resource, get_boston_wards, get_boston_wards, get_boston_wards)

        repo.logout()

        return doc

boston_wards.execute()
doc = boston_wards.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
