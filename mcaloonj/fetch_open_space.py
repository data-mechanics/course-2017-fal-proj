import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fetch_open_space(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = []
    writes = ['mcaloonj.open_space']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson" #ArcGIS Open Data
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)

        repo.dropCollection("mcaloonj.open_space")
        repo.createCollection("mcaloonj.open_space")
        repo["mcaloonj.open_space"].insert_many(r["features"])


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

        repo.authenticate('mcaloonj', 'mcaloonj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:mcaloonj#fetch_open_space', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7.geojson', {'prov:label':'Open Spaces', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_open_space = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_open_space, this_script)

        doc.usage(get_open_space, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        open_space = doc.entity('dat:mcaloonj#open_space', {prov.model.PROV_LABEL:'Open Spaces', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_space, this_script)
        doc.wasGeneratedBy(open_space, get_open_space, endTime)
        doc.wasDerivedFrom(open_space, resource, get_open_space, get_open_space, get_open_space)

        repo.logout()

        return doc
'''
fetch_open_space.execute()
doc = fetch_open_space.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
