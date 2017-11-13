import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from builtins import staticmethod

class trafficData(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = []
    writes = ['bohorqux_peterg04_rocksdan_yfchen.traffic']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the db connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        
        url = 'https://data.cityofboston.gov/resource/dih6-az4h.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("traffic")
        repo.createCollection("traffic")
        repo['bohorqux_peterg04_rocksdan_yfchen.traffic'].insert_many(r)
        repo['bohorqux_peterg04_rocksdan_yfchen.traffic'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.traffic'].metadata())
        
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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/Transportation/')

        this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#trafficData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_trafficData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_trafficData, this_script)
        doc.usage(get_trafficData, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        trafficData= doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#trafficData', {prov.model.PROV_LABEL:'Traffic', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficData, this_script)
        doc.wasGeneratedBy(trafficData, get_trafficData, endTime)
        doc.wasDerivedFrom(trafficData, resource, get_trafficData, get_trafficData, get_trafficData)

        repo.logout()
                  
        return doc
        
        
        
    
    
