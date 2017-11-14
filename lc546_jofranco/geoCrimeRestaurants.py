import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
class geoCrimeRestaurants(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.crimerate']
    writes = ['lc546_jofranco.CrimeRestaurants']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        '''Find list of restaurants that are near the hubway stations'''
        #print(repo['lc546_jofranco.hubway'].find())
        hubs = repo.lc546_jofranco.crimerate
        #print(hubs.find())
        #food = repo.lc546_jofranco.permitgeodata
        #print("food", food.find())
        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        df = pd.read_json(url)
        #print("this", response)
        zip = df[['location']]
        zip.columns = ['location']
        r = json.loads(zip.to_json(orient='records'))
        #r = json.loads(response)
        #print("$$$$$", r)
        foodlocale = list()
        # for locations in r:
        #     coor = {
        #     'location': locations['location']['coordinates']
        #     }
        #     foodlocale.append(coor)
        #print(foodlocale)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropCollection("crimefoodgeodata")
        repo.createCollection("crimefoodgeodata")
        repo["lc546_jofranco.crimefoodgeodata"].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        repo["lc546_jofranco.crimefoodgeodata"].insert_many(r)
        repo["lc546_jofranco.crimefoodgeodata"].metadata({'complete':True})
        #print(foodlocale)

        restaurants_near_crime = []
        for i in hubs.find():
            #print(i)
            #print(i["lo"])
            restaurants = len(repo.command(
            'geoNear','lc546_jofranco.crimefoodgeodata',
             near = { 'coordinates':[float(i['location']["longitude"]), float(i['location']["latitude"])]},
             spherical = True,
             maxDistance= 500)['results'])
            food_crime = {}
            food_crime['numberRestaurantsnear'] = restaurants
            food_crime['location'] = [i['location']["longitude"], i['location']["latitude"]]
            restaurants_near_crime.append(food_crime)

        print("#######KACHOW",restaurants_near_crime)
        s = json.dumps(restaurants_near_crime, sort_keys=True, indent=2)
        repo.dropCollection("CrimeRestaurants")
        repo.createCollection("CrimeRestaurants")
        #repo["lc546_jofranco.HubwayRestaurants"].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        repo["lc546_jofranco.CrimeRestaurants"].insert_many(restaurants_near_crime)
        repo["lc546_jofranco.CrimeRestaurants"].metadata({'complete':True})
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

geoCrimeRestaurants.execute()
doc = geoCrimeRestaurants.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
