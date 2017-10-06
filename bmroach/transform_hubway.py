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
    reads = ['bmroach.hubway']
    writes = []

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
                if log:
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
        
        #Aggregate number of trips by neighborhood'
        agg = {}
        for trip in newEntriesList:
            neighborhood = trip['Neighborhood'] 
            if neighborhood in agg:
                agg[neighborhood] += 1
            else:
                agg[neighborhood] = 1


        with open("./custom_output_datasets/hubway_by_neighborhood.json", 'w') as outfile:
            json.dump(agg, outfile)

        
        
        
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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.        

        this_script = doc.agent('alg:bmroach#transform_hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('dat:bmroach#hubway', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        transform = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(transform, this_script)
        
        doc.usage(transform,resource, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':''  
                            }
                            )
        
        

        hubway_by_neighborhood = doc.entity('dat:bmroach#hubway_by_neighborhood', {prov.model.PROV_LABEL:'hubway_by_neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway_by_neighborhood, this_script)
        doc.wasGeneratedBy(hubway_by_neighborhood, transform, endTime)
        
        doc.wasDerivedFrom(hubway_by_neighborhood, resource, transform, transform, transform)
      
        repo.logout()                  
        return doc
        
                  
# transform_hubway.execute(cacheIn=False, cacheOut=False, log=False)

# doc = transform_hubway.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
