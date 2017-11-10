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
        print("RUNNING")
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
           
            bikeCoords.append((entry['geometry']['type'],entry['geometry']['coordinates']))
            

        for x in bikeCoords:
            y = x[1]
            if type(y[0]) is list:
                for coordinate in y:
                    if type(coordinate[0]) is list:
                        for realcoordinate in coordinate:
                            bikeCoordsTuple.append((entry['geometry']['type'], realcoordinate))
                    else:
                        bikeCoordsTuple.append((entry['geometry']['type'],coordinate))
            else:
                
                bikeCoordsTuple.append((entry['geometry']['type'],y))




        
        


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
            #print(entry['geometry']['coordinates'][0])
            #print("FUCK")
           
         
            
            for coordinate in entry['geometry']['coordinates'][0]:
                #print(coordinate)
                coordinateArray.append((entry['properties']['Name'],coordinate))
            
            y = z(entry)
            neighborhoodCoords.append(y)
        
        #print(neighborhoodCoordsTuple)
        
        definiteNeighborhoodCoordinates = []
    
        for x in coordinateArray:
            y = x[1]
            if type(y[0]) is list:
                for coordinate in y:
                    definiteNeighborhoodCoordinates.append((x[0],coordinate,0,0,0,0))
            else:
                definiteNeighborhoodCoordinates.append((x[0], y, 0,0,0,0))


       

        for (i,tup) in enumerate(definiteNeighborhoodCoordinates):
            a,b,c,d,e,f = tup
           
            coords = (round(b[0],3), round(b[1],3))

            
            for (j,tup2) in enumerate(chargingstationTuple):
                y,z = tup2
                coords2 = (round(z[0],3), round(z[1],3))
           
               
                #coords2[1]= float(str(coords2[1])[:-1])
               
               
                if coords == coords2:
                    definiteNeighborhoodCoordinates[i] = a,b,1,d,e,f
                    
                    

        for (i,tup) in enumerate(definiteNeighborhoodCoordinates):
            a,b,c,d,e,f = tup
        
            coords = (round(b[0],3), round(b[1],3))
            
            
            for (j,tup2) in enumerate(hubwayCoordsTuple):
                y,z = tup2
                coords2 = (round(z[0],3), round(z[1],3))
                
                
                #coords2[1]= float(str(coords2[1])[:-1])
                
                
                if coords == coords2:
                    definiteNeighborhoodCoordinates[i] = a,b,c,1,e,f

        abridgedTuple = bikeCoordsTuple[:8000]
        print(len(definiteNeighborhoodCoordinates))
        abridgedCoords = definiteNeighborhoodCoordinates[:5000]
        for (i,tup) in enumerate(abridgedCoords):
            a,b,c,d,e,f = tup
            #print(len(definiteNeighborhoodCoordinates))

            coords = (round(b[0],3), round(b[1],3))
            
            
            for (j,tup2) in enumerate(abridgedTuple):
                y,z = tup2
                assert(len(z) == 2 and type(z[0] is int) and type(z[1] is int))
                coords2 = (round(z[0],3), round(z[1],3))
                
                
                #coords2[1]= float(str(coords2[1])[:-1])
                
                
                if coords == coords2:
                    definiteNeighborhoodCoordinates[i] = a,b,c,d,1,f
                    print("FUCKING MATCH")



        print(definiteNeighborhoodCoordinates)

        
        



                   


        
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
                if type(coordinate[0]) is list:
                    for realcoordinate in coordinate:
                        if type(realcoordinate[0]) is list:
                            for realreal in realcoordinate:
                                openspaceCoords.append((entry['properties']['SITE_NAME'], realreal))
                        else:
                            openspaceCoords.append((entry['properties']['SITE_NAME'], realcoordinate))
                else:
                    openspaceCoords.append((entry['properties']['SITE_NAME'],coordinate))
                        
        openspaceAbridged = openspaceCoords[:8000]
        for (i,tup) in enumerate(definiteNeighborhoodCoordinates[:7000]):
            a,b,c,d,e,f = tup
            #print(len(definiteNeighborhoodCoordinates))
            
            coords = (round(b[0],3), round(b[1],3))
            
            
            for (j,tup2) in enumerate(openspaceAbridged):
                y,z = tup2
                #print(z)
                assert(len(z) == 2 and type(z[0] is int) and type(z[1] is int))
                coords2 = (round(z[0],3), round(z[1],3))
                
                
                #coords2[1]= float(str(coords2[1])[:-1])
                
                
                if coords == coords2:
                    definiteNeighborhoodCoordinates[i] = a,b,c,d,e,1
                    print("FUCKING MATCH")



        keys = {x[0] for x in definiteNeighborhoodCoordinates}
    
        aggregate = [(key,
                      [sum([c for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key]),
                      sum([d for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key]),
                      sum([e for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key]),
                      sum([f for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key])]
                      )
                      for key in keys]
        print(aggregate)
        
        
       
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
