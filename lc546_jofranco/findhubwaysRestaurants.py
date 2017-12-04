import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
class findhubwaysRestaurants(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.hubway']
    writes = ['lc546_jofranco.HubwayRestaurants']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        '''Find list of restaurants that are near the hubway stations'''
        hubs = repo.lc546_jofranco.hubway

    #    url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
    #    url = 'https://data.boston.gov/export/f1e/137/f1e13724-284d-478c-b8bc-ef042aa5b70b.json'
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        response = open('/Users/Jesus/Desktop/project1/course-2017-fal-proj/lc546_jofranco/fixedpermits.txt').read()
        #r = json.loads(response)
        #print(response)
        df = pd.read_json(response)
        print(df)

        zip = df['food']
        zip.columns = ['Location']
        r = json.loads(zip.to_json(orient='records'))
        foodlocale = list()

        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropCollection("permitgeodata")
        repo.createCollection("permitgeodata")
        repo["lc546_jofranco.permitgeodata"].ensure_index([("Location", dml.pymongo.GEOSPHERE)])
        repo["lc546_jofranco.permitgeodata"].insert_many(r)
        repo["lc546_jofranco.permitgeodata"].metadata({'complete':True})

        restaurants_near_hubway = []
        for i in hubs.find():
            restaurants = len(repo.command(
            'geoNear','lc546_jofranco.permitgeodata',
             near = { 'coordinates':[i["la"], i["lo"]]},
             spherical = True,
             maxDistance= 5000)['results'])
            food_bike = {}
            food_bike['numberRestaurantsnear'] = restaurants
            food_bike['location'] = [float(i["la"]), float(i["lo"])]
            restaurants_near_hubway.append(food_bike)

        s = json.dumps(restaurants_near_hubway, sort_keys=True, indent = 2)
        repo.dropCollection("HubwayRestaurants")
        repo.createCollection("HubwayRestaurants")
        print(restaurants_near_hubway)
        repo["lc546_jofranco.HubwayRestaurants"].insert_many(restaurants_near_hubway)
        repo["lc546_jofranco.HubwayRestaurants"].metadata({'complete':True})
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://secure.thehubway.com/data/')
        this_script = doc.agent('alg:lc546_jofranco#hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'Hubway nearby', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bikeinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'hubway', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_bikeinfo, this_script)
        doc.usage(get_bikeinfo, resource, startTime)
        Bikeinfo = doc.entity('dat:lc546_jofranco#Bikeinfo', {prov.model.PROV_LABEL:'Hubway Bike info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Bikeinfo, this_script)
        doc.wasGeneratedBy(Bikeinfo, get_bikeinfo, endTime)
        doc.wasDerivedFrom(Bikeinfo, resource, get_bikeinfo, get_bikeinfo, get_bikeinfo)
        return doc

findhubwaysRestaurants.execute()
doc = findhubwaysRestaurants.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
