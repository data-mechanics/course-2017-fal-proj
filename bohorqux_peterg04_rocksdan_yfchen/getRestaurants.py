import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

# Credits
# We are taking this database from eileenli_yidingou from their previous project #1

class getRestaurants(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = []
    writes = ['bohorqux_peterg04_rocksdan_yfchen.Restaurants']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        print("Retrieving getRestaurants...")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        url = 'http://datamechanics.io/data/eileenli_yidingou/Restaurant.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Restaurants")
        repo.createCollection("Restaurants")
        repo['bohorqux_peterg04_rocksdan_yfchen.Restaurants'].insert_many(r)
        repo['bohorqux_peterg04_rocksdan_yfchen.Restaurants'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.Restaurants'].metadata())

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
#         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
#         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
#         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
#         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
# 
#         this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#getRestaurants', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#         resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#         get_Restaurants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#         doc.wasAssociatedWith(get_Restaurants, this_script)
#         doc.usage(get_Restaurants, resource, startTime, None,
#                   {prov.model.PROV_TYPE:'ont:Retrieval',
#                   'ont:Query':'?type=BostonLife+Restaurants&$select=type,latitude,longitude,OPEN_DT'
#                   }
#                   )
# 
#         Restaurants = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#Restaurants', {prov.model.PROV_LABEL:'BostonLife Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
#         doc.wasAttributedTo(Restaurants, this_script)
#         doc.wasGeneratedBy(Restaurants, getRestaurants, endTime)
#         doc.wasDerivedFrom(Restaurants, resource, getRestaurants, getRestaurants, getRestaurants)

        repo.logout()
                  
        return doc

# example.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof