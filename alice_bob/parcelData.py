import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from builtins import staticmethod

class parcelData(dml.Algorithm):
    contributor = 'peterg04_yfchen'
    reads = []
    writes = ['peterg04_yfchen.parcelData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the db connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('peterg04_yfchen', 'peterg04_yfchen')
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/f3d274161b4a47aa9acf48d0d04cd5d4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("parcelData")
        repo.createCollection("parcelData")
        repo['peterg04_yfchen.parcelData'].insert(r)
        repo['peterg04_yfchen.parcelData'].metadata({'complete':True})
        print(repo['peterg04_yfchen.parcelData'].metadata())
        
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
        repo.authenticate('peterg04_yfchen', 'peterg04_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:peterg04_yfchen#parcelData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_parcelData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_parcelData, this_script)
        doc.usage(get_parcelData, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        parcelData= doc.entity('dat:peterg04_yfchen#parcelData', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(parcelData, this_script)
        doc.wasGeneratedBy(parcelData, get_parcelData, endTime)
        doc.wasDerivedFrom(parcelData, resource, get_parcelData, get_parcelData, get_parcelData)

        repo.logout()
                  
        return doc
        
        
        
    
    