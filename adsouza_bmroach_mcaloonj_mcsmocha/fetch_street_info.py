import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fetch_speed_info(dml.Algorithm):
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

        #features, properties, SPEEDLIMIT
        # geometry


        speed_limits_coordinates = []
        for street in r["features"]:
            speed_limit = street["properties"]["SPEEDLIMIT"]
            coordinates = street["geometry"]["coordinates"]
            speed_limits_coordinates.append([speed_limit, coordinates])
        street_dict = {"streets": speed_limits_coordinates}

        print (street_dict)

        repo.dropCollection("adsouza_bmroach_mcaloonj_mcsmocha.street_info")
        repo.createCollection("adsouza_bmroach_mcaloonj_mcsmocha.street_info")
        repo["adsouza_bmroach_mcaloonj_mcsmocha.street_info"].insert(street_dict)


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

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_speed_info', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson', {'prov:label':'Boston Segments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_street_info= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_street_info, this_script)

        doc.usage(get_street_info, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        street_info = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#street_info', {prov.model.PROV_LABEL:'Speed Limits', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(street_info, this_script)
        doc.wasGeneratedBy(street_info, get_street_info, endTime)
        doc.wasDerivedFrom(street_info, resource, get_street_info, get_street_info, get_street_info)

        repo.logout()

        return doc


fetch_speed_info.execute()
doc = fetch_speed_info.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
