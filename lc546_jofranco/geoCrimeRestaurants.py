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

        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        df = pd.read_json(url)
        zip = df[['location']]
        zip.columns = ['location']
        r = json.loads(zip.to_json(orient='records'))
        foodlocale = list()
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropCollection("crimefoodgeodata")
        repo.createCollection("crimefoodgeodata")
        repo["lc546_jofranco.crimefoodgeodata"].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        repo["lc546_jofranco.crimefoodgeodata"].insert_many(r)
        repo["lc546_jofranco.crimefoodgeodata"].metadata({'complete':True})


        restaurants_near_crime = []
        for i in hubs.find():
            restaurants = len(repo.command(
            'geoNear','lc546_jofranco.crimefoodgeodata',
             near = { 'coordinates':[float(i['location']["longitude"]), float(i['location']["latitude"])]},
             spherical = True,
             maxDistance= 500)['results'])
            food_crime = {}
            food_crime['numberRestaurantsnear'] = restaurants
            food_crime['location'] = [i['location']["longitude"], i['location']["latitude"]]
            restaurants_near_crime.append(food_crime)

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/crime.json')
        this_script = doc.agent('alg:lc546_jofranco#geoCrimeRestaurants', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'restaurant nearby', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_geocrimeinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Restaurants near a crime scene', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_geocrimeinfo, this_script)
        doc.usage(get_geocrimeinfo, resource, startTime)
        geoCrimeinfo = doc.entity('dat:lc546_jofranco#geoCrimeRestaurants', {prov.model.PROV_LABEL:'Hubway Bike info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(geoCrimeinfo, this_script)
        doc.wasGeneratedBy(geoCrimeinfo, get_geocrimeinfo, endTime)
        doc.wasDerivedFrom(geoCrimeinfo, resource, get_geocrimeinfo, get_geocrimeinfo, get_geocrimeinfo)
        return doc

geoCrimeRestaurants.execute()
doc = geoCrimeRestaurants.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
