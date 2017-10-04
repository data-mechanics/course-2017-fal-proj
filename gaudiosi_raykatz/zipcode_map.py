import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class zipcode_map(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = []
    writes = ['gaudiosi_katz.zipcode_map']

    @staticmethod
    def execute(trial = False):
        '''Retrieve zipcode_map from City of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        boston_url = "https://data.boston.gov"
        url = "http://gis.cityofboston.gov/arcgis/rest/services/Planning/OpenData/MapServer/1/query?where=1%3D1&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&outFields=*&returnGeometry=true&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&f=pjson"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        result = json.loads(response)
        prelim_r = result["features"]
        r = []
        for points in prelim_r:
            d = {}
            d["zipcode"] = points["attributes"]["ZIP5"]
            d["ShapeSTArea()"] = points["attributes"]["Shape.STArea()"]
            d["ShapeSTLength()"] = points["attributes"] = ["Shape.STLength()"]
            d["geometry"] = points["geometry"]
            r.append(d)

        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("zipcode_map")
        repo.createCollection("zipcode_map")
        repo['gaudiosi_raykatz.zipcode_map'].insert_many(r)
        repo['gaudiosi_raykatz.zipcode_map'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.zipcode_map'].metadata())
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
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gaudiosi_raykatz#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_zipcode_map = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zipcode_map, this_script)
        
        doc.usage(get_zipcode_map, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Map&$select=zipcode,lat,long'
                  }
                  )
        
        zipcode_map = doc.entity('dat:gaudiosi_raykatz#zipcode_map', {prov.model.PROV_LABEL:'Zipcode Map', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zipcode_map, this_script)
        doc.wasGeneratedBy(zipcode_map, get_zipcode_map, endTime)
        doc.wasDerivedFrom(zipcode_map, resource, get_zipcode_map, get_zipcode_map, get_zipcode_map)

        repo.logout()
                  
        return doc

## eof
