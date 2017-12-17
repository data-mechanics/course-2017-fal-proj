import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import geojson
#from tqdm import tqdm
import pdb



class NeighborhoodScores(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    reads = []
    writes = ['francisz_jrashaan.hubways', 'francisz_jrashaan.ChargingStation', 'francisz_jrashaan.bikeNetwork',
              'francisz_jrashaan.openspace', 'francisz_jrashaan.neighborhood', 'francisz_jrashaan.presetneighborhoodScores', 'francisz_jrashaan.neighborhoodscores']
    
    @staticmethod
    def execute(trial=True):
        print("Please allow a few minutes for the algorithm to run")
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
        repo['francisz_jrashaan.bikeNetwork'].insert_many(geoList)
        repo['francisz_jrashaan.bikeNetwork'].metadata({'complete':True})
        bikeCoords = []
        bikeCoordsTuple = []
        for entry in repo.francisz_jrashaan.bikeNetwork.find():
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
                
                
                if coords == coords2:
                    definiteNeighborhoodCoordinates[i] = a,b,c,1,e,f

        abridgedTuple = bikeCoordsTuple[:8000]
        #print(len(definiteNeighborhoodCoordinates))
        abridgedCoords = definiteNeighborhoodCoordinates[:5000]
        for (i,tup) in enumerate(abridgedCoords):
            a,b,c,d,e,f = tup

            coords = (round(b[0],3), round(b[1],3))
            
            
            for (j,tup2) in enumerate(abridgedTuple):
                y,z = tup2
                assert(len(z) == 2 and type(z[0] is int) and type(z[1] is int))
                coords2 = (round(z[0],3), round(z[1],3))
                
                
                
                
                if coords == coords2:
                    definiteNeighborhoodCoordinates[i] = a,b,c,d,1,f



        #print(definiteNeighborhoodCoordinates)

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



        keys = {x[0] for x in definiteNeighborhoodCoordinates}
    
        aggregate = [(key,
                      [sum([c for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key]),
                      sum([d for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key]),
                      sum([e for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key]),
                      sum([f for (a,b,c,d,e,f) in definiteNeighborhoodCoordinates if a == key])]
                      )
                      for key in keys]

        agg1 = []
        for x in aggregate:
             #print(x)
             y = lambda t: ({"Neighborhood":t[0],'Charging Station':t[1][0],'Hubway Stations':t[1][1],'Bike Networks':t[1][2],'Open Space':t[1][3]})
             z = y(x)
             agg1.append(z)
        repo.dropCollection("neighborhoodscores")
        repo.createCollection("neighborhoodscores")
        repo['francisz_jrashaan.neighborhoodscores'].insert_many(agg1)
        repo['francisz_jrashaan.neighborhoodscores'].metadata({'complete':True})

        
        agg2 = []
        #print(aggregate)
        neighborhoodDict = [{'North End': [0, 3, 236, 240]}, {'Bay Village': [0, 0, 24, 42]}, {'East Boston': [0, 19, 222, 3544]}, {'Leather District': [8, 8, 34, 43]}, {'Allston': [0, 1, 1888, 1994]}, {'Hyde Park': [0, 0, 569, 1163]}, {'Roslindale': [0, 0, 450, 608]}, {'Charlestown': [0, 7, 189, 455]}, {'Back Bay': [4, 17, 432, 817]}, {'South End': [0, 0, 116, 150]}, {'Downtown': [4, 33, 160, 420]}, {'Dorchester': [0, 7, 1382, 3710]}, {'South Boston Waterfront': [15, 7, 102, 222]}, {'West Roxbury': [0, 0, 559, 708]}, {'Longwood Medical Area':[0, 11, 136, 154]}, {'Mission Hill': [0, 11, 135, 161]}, {'Roxbury': [0, 7, 315, 525]}, {'Beacon Hill': [1, 16, 149, 391]}, {'Mattapan': [0, 0, 348, 627]}, {'Harbor Islands':[0, 0, 0, 155]}, {'Brighton': [0, 0, 983, 1466]}, {'South Boston':[0, 1, 410, 1061]}, {'West End': [0, 5, 387, 549]}, {'Fenway': [4, 21, 893, 1034]}, {'Chinatown': [11, 21, 74, 112]}, {'Jamaica Plain': [0, 0, 356, 919]}]
        for x in neighborhoodDict:
                name = list(x.keys())[0]
                lst = list(x.values())[0]
                #print(x)
                z= {"Neighborhood":name,'Charging Station':lst[0],'Hubway Stations':lst[1],'Bike Networks':lst[2],'Open Space':lst[3]}
                agg2.append(z)
        
        repo.dropCollection("presetneighborhoodScores")
        repo.createCollection("presetneighborhoodScores")


        repo['francisz_jrashaan.presetneighborhoodScores'].insert_many(agg2)
        repo['francisz_jrashaan.presetneighborhoodScores'].metadata({'complete':True})


       
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
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/')
        
        
        this_script = doc.agent('alg:francisz_jrashaan#NeighborhoodScores', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_chargeStations = doc.entity('bdp:465e00f9632145a1ad645a27d27069b4_2', {'prov:label':'Charging Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_Hubways = doc.entity('bdp:ee7474e2a0aa45cbbdfe0b747a5eb032_0', {'prov:label':'Hubways', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'gepjson'})
        resource_bikeNetwork = doc.entity('bdp:d02c9d2003af455fbc37f550cc53d3a4_0', {'prov:label':'Bike Networks', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_neighborhood = doc.entity('bdp:3525b0ee6e6b427f9aab5d0a1d0a1a28_0', {'prov:label':'Neighborhood Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_openspace = doc.entity('bdp:2868d370c55d4d458d4ae2224ef8cddd_7', {'prov:label':'Open Space', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        
        get_chargeStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Hubways = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bikeNetworks = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_openspace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        compute_score = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)


        doc.wasAssociatedWith(get_chargeStations, this_script)
        doc.wasAssociatedWith(get_Hubways, this_script)
        doc.wasAssociatedWith(get_bikeNetworks, this_script)
        doc.wasAssociatedWith(get_neighborhood, this_script)
        doc.wasAssociatedWith(get_openspace, this_script)
        doc.wasAssociatedWith(compute_score, this_script)



        doc.usage(get_chargeStations, resource_chargeStations, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.usage(get_Hubways, resource_Hubways, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'
        }
        )
        doc.usage(get_bikeNetworks, resource_bikeNetwork, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.usage(get_neighborhood, resource_neighborhood, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.usage(get_openspace, resource_openspace, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'
        }
        )
        doc.usage(compute_score, resource_neighborhood, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})



        
        chargeStations = doc.entity('dat:francisz_jrashaan#ChargingStation', {prov.model.PROV_LABEL:'chargeStations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(chargeStations, this_script)
        doc.wasGeneratedBy(chargeStations, get_chargeStations, endTime)
        doc.wasDerivedFrom(chargeStations, resource_chargeStations, get_chargeStations, get_chargeStations, get_chargeStations)
      
        Hubways = doc.entity('dat:francisz_jrashaan#hubways', {prov.model.PROV_LABEL:'Hubways', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Hubways, this_script)
        doc.wasGeneratedBy(Hubways, get_Hubways, endTime)
        doc.wasDerivedFrom(Hubways, resource_Hubways, get_Hubways, get_Hubways, get_Hubways)
      
        bikeNetwork = doc.entity('dat:francisz_jrashaan#bikeNetwork', {prov.model.PROV_LABEL:'bikeNetwork', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeNetwork, this_script)
        doc.wasGeneratedBy(bikeNetwork, get_bikeNetworks, endTime)
        doc.wasDerivedFrom(bikeNetwork, resource_bikeNetwork, get_bikeNetworks, get_bikeNetworks, get_bikeNetworks)
      
        neighborhood = doc.entity('dat:francisz_jrashaan#neighborhood', {prov.model.PROV_LABEL:'Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood, this_script)
        doc.wasGeneratedBy(neighborhood, get_neighborhood, endTime)
        doc.wasDerivedFrom(neighborhood, resource_neighborhood, get_neighborhood, get_neighborhood, get_neighborhood)
      
        openspace = doc.entity('dat:francisz_jrashaan#openspace', {prov.model.PROV_LABEL:'openspace', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(openspace, this_script)
        doc.wasGeneratedBy(openspace, get_openspace, endTime)
        doc.wasDerivedFrom(openspace, resource_openspace, get_openspace, get_openspace, get_openspace)
      

        neighborhoodscores = doc.entity('dat:francisz_jrashaan#neighborhoodscores', {prov.model.PROV_LABEL:'Computed Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoodscores, this_script)
        doc.wasGeneratedBy(neighborhoodscores, compute_score, endTime)
        doc.wasDerivedFrom(neighborhoodscores, neighborhood, compute_score, compute_score, compute_score)

        presetneighborhoodScores = doc.entity('dat:francisz_jrashaan#presetneighborhoodScores', {prov.model.PROV_LABEL:'Precomputed Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(presetneighborhoodScores, this_script)
        doc.wasGeneratedBy(presetneighborhoodScores, compute_score, endTime)
        doc.wasDerivedFrom(presetneighborhoodScores, neighborhood, compute_score, compute_score, compute_score)
         

        repo.logout()
          
        return doc

NeighborhoodScores.execute()
doc = NeighborhoodScores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
