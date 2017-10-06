import urllib.request
import json
import dml, prov.model
import datetime, uuid
import geojson

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

Aggregate Hubway trips by Boston neighborgood

Development notes:


"""

class transform_hubway(dml.Algorithm):
    contributor = 'bmroach'
    reads = ['bmroach.']
    writes = ['bmroach.']

    @staticmethod
    def execute(trial = False, log=False, cacheIn=False, cacheOut=False):
        startTime = datetime.datetime.now()

        #import keys
        with open('../auth.json') as json_data:
            auth = json.load(json_data)

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        #Read in Hubway data
        hubway = repo.bmroach.hubway.find() 
        locationCache={}

        if cacheIn: #read in cached locations from local file to minimize API calls
            with open('locationCache.json') as inFile:
                locationCache = json.load(inFile)

        idVal=0
        newEntriesList = []
        for entry in hubway:
            entry=entry[str(idVal)]
            idVal += 1
            #prefer to use start, but can use end. If neither, pass
            sLat = entry["Start Station Latitude"]
            sLng = entry["Start Station Longitude"]

            eLat = entry["End Station Latitude"]
            eLng = entry["End Station Longitude"]

            # station not included in imported dataset
            if sLat == '0' or sLng == '0':
                if eLat == '0' or eLng == '0':
                    continue #skip this trip
                else:
                    finalPair = (eLat, eLng)
            else:
                finalPair = (sLat, sLng)
            
            #Check if in cache
            if str(finalPair) in locationCache:
                if log:
                    print("Cache hit for", finalPair)
                geocodeLocation = locationCache[ str(finalPair) ]

            #Cache miss, so retrieve
            else:
                print("making api call for", finalPair)
                url = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" \
                + str( finalPair[0] ) + ','+ str( finalPair[1] ) \
                + "&key=" + auth['GoogleMapsAPI']['key']

                geocodeLocation = urllib.request.urlopen(url).read().decode("utf-8")
                locationCache[ str(finalPair) ] = geocodeLocation # store in the cache to save out    
            
            

            #extract the neighborhood name
            geocodeLocation = json.loads(geocodeLocation)            
            neighborhood = geocodeLocation['results'][0]['address_components'][2]['long_name']
            if log:
                print(neighborhood, "extracted")
        
            #assemble the new data
            newEntriesList.append({
                                "Start Date": entry["Start Date"],
                                "Member Type": entry["Member Type"],
                                "Neighborhood": neighborhood,
                                })
            
            # *** End of Entries For Loop ***

        
        # write out to disk cache
            if cacheOut:
                with open('locationCache.json', 'w') as outfile:
                    json.dump(locationCache, outfile)
        
        print(newEntriesList)   

        #Aggregate number of trips by neighborhood'
        





        
        return
        
        # New collection
        newCollectionName = "hubway_by_neighborhood"
        repo.dropCollection(newCollectionName)
        repo.createCollection(newCollectionName)    
                
        repo['bmroach.'+newCollectionName].insert_many(  )
        repo['bmroach.'+newCollectionName].metadata({'complete':True})  
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

        
                  
        return 





transform_hubway.execute(cacheIn=True, cacheOut=True, log=True)

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
