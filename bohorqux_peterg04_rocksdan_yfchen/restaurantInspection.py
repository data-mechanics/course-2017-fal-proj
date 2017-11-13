import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from builtins import staticmethod

class restaurantInspection(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = []
    writes = ['bohorqux_peterg04_rocksdan_yfchen.restaurants']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the db connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        
        url = 'https://data.cityofboston.gov/resource/427a-3cn5.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("restaurants")
        repo.createCollection("restaurants")
        repo['bohorqux_peterg04_rocksdan_yfchen.restaurants'].insert_many(r)
        repo['bohorqux_peterg04_rocksdan_yfchen.restaurants'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.restaurants'].metadata())
        
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/Health/')

        this_script = doc.agent('alg:peterg04_yfchen#restaurantInspection', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_restaurantInspection = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_restaurantInspection, this_script)
        doc.usage(get_restaurantInspection, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        restaurantInspection= doc.entity('dat:peterg04_yfchen#restaurantInspection', {prov.model.PROV_LABEL:'Restaurant Density', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(restaurantInspection, this_script)
        doc.wasGeneratedBy(restaurantInspection, get_restaurantInspection, endTime)
        doc.wasDerivedFrom(restaurantInspection, resource, get_restaurantInspection, get_restaurantInspection, get_restaurantInspection)

        repo.logout()
                  
        return doc
        
        

    
    
