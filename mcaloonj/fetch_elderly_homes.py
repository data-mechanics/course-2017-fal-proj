import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fetch_elderly_homes(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = []
    writes = ['mcaloonj.elderly_homes']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        url = "http://services.arcgis.com/sFnw0xNflSi8J0uh/ArcGIS/rest/services/ElderyHousing/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Project_Name%2C+Housing_Type%2C+Parcel_Address%2C+MatchLatitude%2C+MatchLongitude&returnGeometry=true&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnDistinctValues=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="

        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)

        s = json.dumps(r, sort_keys=True, indent=2)
        #print (s)

        repo.dropCollection("mcaloonj.elderly_homes")
        repo.createCollection("mcaloonj.elderly_homes")
        repo["mcaloonj.elderly_homes"].insert_many(r["features"])

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('mcaloonj', 'mcaloonj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('arc', 'http://services.arcgis.com/sFnw0xNflSi8J0uh/ArcGIS/rest/services/ElderyHousing/FeatureServer/0/query')

        this_script = doc.agent('alg:mcaloonj#fetch_elderly_homes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        resource = doc.entity('arc:'+str(uuid.uuid4()), {'prov:label': 'Elderly Housing', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        get_elderly_homes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_elderly_homes, this_script)
        doc.usage(get_elderly_homes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Project_Name%2C+Housing_Type%2C+Parcel_Address%2C+MatchLatitude%2C+MatchLongitude&returnGeometry=true&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnDistinctValues=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='})

        elderly_homes = doc.entity('dat:mcaloonj#elderly_homes',{prov.model.PROV_LABEL:'Elderly Home Locations',prov.model.PROV_TYPE:'ont:DataSet'} )

        doc.wasAttributedTo(elderly_homes, this_script)
        doc.wasGeneratedBy(elderly_homes, get_elderly_homes, endTime)
        doc.wasDerivedFrom(elderly_homes, resource, get_elderly_homes, get_elderly_homes, get_elderly_homes)

        repo.logout()
        return doc

'''
fetch_elderly_homes.execute()
doc = fetch_elderly_homes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
