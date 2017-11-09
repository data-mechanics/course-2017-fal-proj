import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import geojson
from tqdm import tqdm
import pdb



class fzjr_retrievalalgorithm(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    reads = []
    writes = ['francisz_jrashaan.hubways', 'francisz_jrashaan.ChargingStation', 'francisz_jrashaan.bikeNetwork',
              'francisz_jrashaan.openspace', 'francisz_jrashaan.neighborhood']
    
    @staticmethod
    def execute(trial=False):
        
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
        repo.dropCollection("hubways")
        repo.createCollection("hubways")
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        # print(geoList)
        
      
        repo['francisz_jrashaan.hubways'].insert_many(geoList)
        repo['francisz_jrashaan.hubways'].metadata({'complete':True})
        hubwaysCoords = []
        hubwayCoordsTuple = []
        for entry in repo.francisz_jrashaan.hubways.find():
            #print(entry)
            z = lambda t:({t['geometry']['type']: (t['geometry']['coordinates'])})
            y = z(entry)
            hubwaysCoords.append(y)
            hubwayCoordsTuple.append((entry['geometry']['type'],entry['geometry']['coordinates']))
        
        # print(hubwayCoordsTuple)
        # print("HUBWAY COORDINATES")
        
        
        #print(hubwaysCoords)
        #print("STOP HERE")
        # print(repo['francisz_jrashaan.crime'].metadata())

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/465e00f9632145a1ad645a27d27069b4_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
       
        repo.dropCollection("ChargingStation")
        repo.createCollection("ChargingStation")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['francisz_jrashaan.ChargingStation'].insert_many(geoList)
        repo['francisz_jrashaan.ChargingStation'].metadata({'complete':True})
        chargingstationCoords = []
        chargingstationTuple = []
        for entry in repo.francisz_jrashaan.ChargingStation.find():
            z = lambda t:({t['geometry']['type']: (t['geometry']['coordinates'])})
            y = z(entry)
            chargingstationCoords.append(y)
            chargingstationTuple.append((entry['geometry']['type'],entry['geometry']['coordinates']))
        
        #print(chargingstationTuple)
        #print("NEW PRINT")
        #print("Coordinates")
        #print("NEXT SEQUENCE")
        
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        repo.dropCollection("bikeNetwork")
        repo.createCollection("bikeNetwork")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['francisz_jrashaan.bikeNetworks'].insert_many(geoList)
        repo['francisz_jrashaan.bikeNetworks'].metadata({'complete':True})
        bikeCoords = []
        bikeCoordsTuple = []
        for entry in repo.francisz_jrashaan.bikeNetworks.find():
            #print(entry)
            z = lambda t:({t['geometry']['type']: (t['geometry']['coordinates'])})
            y = z(entry)
            bikeCoords.append(y)
            for individualCoordinate in entry['geometry']['coordinates']:
            
                bikeCoordsTuple.append(("Point",individualCoordinate))
            
        # print(entry['geometry']['coordinates'])
        
        # print("GOD SEND")
        
        
        # print(bikeCoordsTuple)
        
        


        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
       
        repo.dropCollection("neighborhood")
        repo.createCollection("neighborhood")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        
        
        repo['francisz_jrashaan.neighborhood'].insert(geoList)
        repo['francisz_jrashaan.neighborhood'].metadata({'complete':True})
        #print(repo['francisz_jrashaan.neighborhood'])
        neighborhoodCoords = []
        neighborhoodCoordsTuple = []
        coordinateArray = []
        for entry in repo.francisz_jrashaan.neighborhood.find():
            z = lambda t:({t['properties']['Name']: (t['geometry']['coordinates'])})
            neighborhoodCoordsTuple.append((entry['properties']['Name'],entry['geometry']['coordinates']))
            
           
         
            
            for coordinate in entry['geometry']['coordinates']:
                coordinateArray.append((entry['properties']['Name'],coordinate))
            y = z(entry)
            neighborhoodCoords.append(y)
        
        
        definiteNeighborhoodCoordinates = []
        for x in coordinateArray:
            y = x[1]
            for coordinate in y:
                lastUnpack = coordinate
                for w in lastUnpack:
                    definiteNeighborhoodCoordinates.append((x[0],w,0,0,0,0))
       
       
        for (i,tup) in enumerate(definiteNeighborhoodCoordinates):
            a,b,c,d,e,f = tup
            coords = b
            
            for (j,tup2) in enumerate(chargingstationTuple):
                y,z = tup2
                coords2 = z
               
               
                #coords2[1]= float(str(coords2[1])[:-1])
                print(coords)
                print(coords2)
                print(coords == coords2)
                print("comparin")
                if coords == coords2:
                    print("FOUND IT")
                    definiteNeighborhoodCoordinates[i] = a,b,1,c,d,e,f
                    print("FOUND ONE")

#print(definiteNeighborhoodCoordinates)

        
        #generating neighborhood dictionary with coords
        
# heres what i want to do, compare two tuples and if there is a match update the value at current index of the tuple

                                       
        # repo['francisz_jrashaan.capopulation'].metadata({'complete':True})
        # print(repo['francisz_jrashaan.capopulation'].metadata())
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo.dropCollection("openspace")
        repo.createCollection("openspace")
        repo['francisz_jrashaan.openspace'].insert_many(geoList)
        repo['francisz_jrashaan.openspace'].metadata({'complete':True})
        openspaceCoords = []
        for entry in repo.francisz_jrashaan.openspace.find():
            z = lambda t:({t['properties']['SITE_NAME']: (t['geometry']['coordinates'])})
            y = z(entry)
            
            for coordinate in entry['geometry']['coordinates']:
                openspaceCoords.append((entry['properties']['SITE_NAME'],coordinate))
                        
        '''
                definiteopenCoordinates = []
                for x in openspaceCoords:
                    y = x[1]
                    for coordinate in y:
                        lastUnpack = coordinate
                        for w in lastUnpack:
                            definiteopenCoordinates.append((x[0],w))

        '''


        
        
#for coordinates in range(len(hubwaysCoords)):
#           print (hubwaysCoords[coordinates].values())
#           for coordinates2 in range(len(neighborhoodCoords)):
            
            #               if hubwaysCoords[coordinates].values() in neighborhoodCoords[coordinates2].values():
#       print("yo")
            
            #   for x in coordinates:
                
                #   if x in openspaceCoords.values():
                #   print("yo yo yo")
                
        # print(openspaceCoords.items())
        
        
        #for x in hubwaysCoords.values():
            # print("SUP")
            #print(x)
            # if x in neighborhoodCoords.values():
                # print(neighborhoodCoords.values())
                # print("X IS IN THERE")
        
#print("IS THIS EVEN WORKING")
        #  R = [[i for i in neighborhoodCoords[x]] for x in neighborhoodCoords.keys()]
        # print(R)
        #testV = neighborhoodCoords.values()
        #for X in
        #X = [(k,1) for (k,v) in testV if v in neighborhoodCoords.values()]
        #print(X)

        
         
        
        
       
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cam','https://data.cambridgema.gov/api/views/')
        
        
        this_script = doc.agent('alg:francisz_jrashaan#fzjr_retrievalalgorithm', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crime = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_streetlights = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5', {'prov:label':'Streetlights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_landuse = doc.entity('cam:srp4-fhjz/rows.json', {'prov:label':'Landuse', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_capopulation = doc.entity('cam:r4pm-qqje/rows', {'prov:label':'Cambridge Population Density', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_openspace = doc.entity('bdp:769c0a21-9e35-48de-a7b0-2b7dfdefd35e', {'prov:label':'Openspace', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_landuse = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_capopulation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_openspace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.wasAssociatedWith(get_landuse, this_script)
        doc.wasAssociatedWith(get_capopulation, this_script)
        doc.wasAssociatedWith(get_openspace, this_script)
        doc.usage(get_crime, resource_crime, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.usage(get_streetlights, resource_streetlights, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'
        }
        )
          
        doc.usage(get_landuse, resource_landuse, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.usage(get_capopulation, resource_capopulation, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.usage(get_openspace, resource_openspace, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'
        }
        )
        crime = doc.entity('dat:francisz_jrashaan#crime', {prov.model.PROV_LABEL:'crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_crime, get_crime, get_crime, get_crime)
      
        streetlights = doc.entity('dat:francisz_jrashaan#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetlights, this_script)
        doc.wasGeneratedBy(streetlights, get_streetlights, endTime)
        doc.wasDerivedFrom(streetlights, resource_streetlights, get_streetlights, get_streetlights, get_streetlights)
      
        landuse = doc.entity('dat:francisz_jrashaan#landuse', {prov.model.PROV_LABEL:'landuse', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(landuse, this_script)
        doc.wasGeneratedBy(landuse, get_landuse, endTime)
        doc.wasDerivedFrom(landuse, resource_landuse, get_landuse, get_landuse, get_landuse)
      
        capopulation = doc.entity('dat:francisz_jrashaan#capopulation', {prov.model.PROV_LABEL:'capopulation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(capopulation, this_script)
        doc.wasGeneratedBy(capopulation, get_capopulation, endTime)
        doc.wasDerivedFrom(capopulation, resource_capopulation, get_capopulation, get_capopulation, get_capopulation)
      
        openspace = doc.entity('dat:francisz_jrashaan#openspace', {prov.model.PROV_LABEL:'openspace', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(openspace, this_script)
        doc.wasGeneratedBy(openspace, get_openspace, endTime)
        doc.wasDerivedFrom(openspace, resource_openspace, get_openspace, get_openspace, get_openspace)
      
        repo.logout()
          
        return doc

fzjr_retrievalalgorithm.execute()
doc = fzjr_retrievalalgorithm.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
