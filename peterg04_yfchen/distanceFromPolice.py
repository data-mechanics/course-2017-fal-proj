import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from builtins import staticmethod

class distanceFromPolice(dml.Algorithm):
    contributor = 'peterg04_yfchen'
    reads = ['peterg04_yfchen.policeStations', 'peterg04_yfchen.restaurants']
    writes = ['peterg04_yfchen.distanceFromPolice']
        
    @staticmethod
    def execute(trial = False):
        # helper functions from lecture 591 by Lapets
        def select(R, s):
            return [t for t in R if s(t)]
        
        def project(R, p):
            return [p(t) for t in R]
        
        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k,v) in R if k == key])) for key in keys]
        
        def intersect(R, S):
            return [t for t in R if t in S]
        
        def union(R, S):
            return R + S
        
        def product(R, S):
            return [(t,u) for t in R for u in S]
        
        startTime = datetime.datetime.now()
        
        # Set up the db connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('peterg04_yfchen', 'peterg04_yfchen')
        
#         response = urllib.request.urlopen(url).read().decode("utf-8")
#         r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("distanceFromPolice")
        repo.createCollection("distanceFromPolice")
        
        # set the variabes - one for each collection read for manipulation
        police = repo[distanceFromPolice.reads[0]].find()  # a list of dictionaries
        restaurants = repo[distanceFromPolice.reads[1]].find()
        
        # The end result will be a dataset of the format {zipCode from restaurant, restaurantCoordinates, policeStationCoordinates} 
        
        # We will want to do selection on restaurants based on their location coordinates. This will get rid of multiple entries by same restaurant
        # We will then project the data from the restaurants so that we have {"zip":12345, "location" : (xyz,xyz)}
        newRestaurants = select(restaurants, lambda entry: "zip" in entry and "location" in entry)
        
        projectRestaurants = project(newRestaurants, lambda entry: (entry["zip"], entry["location"]["coordinates"]))
        
        # Continuing onto police stations, we will do a projection so that we grab only the coordinates in zip code : (x,y) form
        selectPolice = select(police, lambda t: "properties" in t['features'][0])  
        
        # Need to project and filter out the extra geoJSON data format
        projectPolice = []
        for i in range(0,13):
            projectPolice += project(selectPolice, lambda t: t['features'][i])
        
        projectPolice2 = project(projectPolice, lambda t: (t['properties']['ZIP'], t['geometry']['coordinates']))
        
        # Now that police and restaurants are both in the format of (zipcode, loc_coords) - we can combine the two sets
        # First product to put the two sets together
        productSets = product(projectRestaurants, projectPolice2)
        # Select in order to only choose the ones where the zipcodes match
        refineProduct = select(productSets, lambda t: t[0][0] == t[1][0])
        # Now project them to put them into the final set
        finalProject = project(refineProduct, lambda t: (t[0][0], t[0][1], t[1][1]))
        
        # Convert to dictionary entry and insert into mongo
        finalData = project(finalProject, lambda t: dict([("zip", t[0]), ("location", (t[1], t[2]))]))

        repo['peterg04_yfchen.distanceFromPolice'].insert(finalData)
        repo['peterg04_yfchen.distanceFromPolice'].metadata({'complete':True})
        print(repo['peterg04_yfchen.distanceFromPolice'].metadata())
        
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/Health/')
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:peterg04_yfchen#distanceFromPolice', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        get_distanceFromPolice = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_distanceFromPolice, this_script)
        doc.usage(get_distanceFromPolice, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        distanceFromPolice= doc.entity('dat:peterg04_yfchen#distanceFromPolice', {prov.model.PROV_LABEL:'Distance From Police', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(distanceFromPolice, this_script)
        doc.wasGeneratedBy(distanceFromPolice, get_distanceFromPolice, endTime)
        doc.wasDerivedFrom(distanceFromPolice, resource, get_distanceFromPolice, get_distanceFromPolice, get_distanceFromPolice)

        repo.logout()
                  
        return doc
        
        
        
    
    