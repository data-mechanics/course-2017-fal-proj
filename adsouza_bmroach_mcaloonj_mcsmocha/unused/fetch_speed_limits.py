import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fetch_speed_limits(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = []
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.speed_limits']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson" #ArcGIS Open Data
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print (s)

        repo.dropCollection("adsouza_bmroach_mcaloonj_mcsmocha.speed_limits")
        repo.createCollection("adsouza_bmroach_mcaloonj_mcsmocha.speed_limits")
        repo["adsouza_bmroach_mcaloonj_mcsmocha.speed_limits"].insert_many(r["features"])


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

        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_speed_limits', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson', {'prov:label':'Boston Segments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_speed_limits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_speed_limits, this_script)

        doc.usage(get_speed_limits, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        speed_limits = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#speed_limits', {prov.model.PROV_LABEL:'Speed Limits', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(speed_limits, this_script)
        doc.wasGeneratedBy(speed_limits, get_speed_limits, endTime)
        doc.wasDerivedFrom(speed_limits, resource, get_speed_limits, get_speed_limits, get_speed_limits)

        repo.logout()

        return doc

'''
fetch_speed_limits.execute()
doc = fetch_speed_limits.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
