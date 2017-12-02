import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class zipcode_map(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = []
    writes = ['raykatz_nedg_gaudiosi.zipcode_map']

    @staticmethod
    def execute(trial = False):
        '''Retrieve zipcode_map from City of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/53ea466a189b4f43b3dfb7b38fa7f3b6_1.geojson"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("zipcode_map")
        repo.createCollection("zipcode_map")
        repo['raykatz_nedg_gaudiosi.zipcode_map'].insert(r)
        repo['raykatz_nedg_gaudiosi.zipcode_map'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.zipcode_map'].metadata())
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
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdpmap', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdpmap:53ea466a189b4f43b3dfb7b38fa7f3b6_1', {'prov:label':'Boston zipcode data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_zipcode_map = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zipcode_map, this_script)
        
        doc.usage(get_zipcode_map, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        
        zipcode_map = doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_map', {prov.model.PROV_LABEL:'Zipcode Map', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zipcode_map, this_script)
        doc.wasGeneratedBy(zipcode_map, get_zipcode_map, endTime)
        doc.wasDerivedFrom(zipcode_map, resource, get_zipcode_map, get_zipcode_map, get_zipcode_map)

        repo.logout()
                  
        return doc
'''
zipcode_map.execute()
doc = zipcode_map.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
