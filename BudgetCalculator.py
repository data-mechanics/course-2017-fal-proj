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
import scipy.stats
import z3

class BudgetCalculator(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    
    reads = ['francisz_jrashaan.neighborhoodScores']
    
    writes = ['francisz_jrashaan.optimalScore']
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')
        data = [('North End', [0, 3, 236, 240]), ('Bay Village', [0, 0, 24, 42]), ('East Boston', [0, 19, 222, 3544]), ('Leather District', [8, 8, 34, 43]), ('Allston', [0, 1, 1888, 1994]), ('Hyde Park', [0, 0, 569, 1163]), ('Roslindale', [0, 0, 450, 608]), ('Charlestown', [0, 7, 189, 455]), ('Back Bay', [4, 17, 432, 817]), ('South End', [0, 0, 116, 150]), ('Downtown', [4, 33, 160, 420]), ('Dorchester', [0, 7, 1382, 3710]), ('South Boston Waterfront', [15, 7, 102, 222]), ('West Roxbury', [0, 0, 559, 708]), ('Longwood Medical Area', [0, 11, 136, 154]), ('Mission Hill', [0, 11, 135, 161]), ('Roxbury', [0, 7, 315, 525]), ('Beacon Hill', [1, 16, 149, 391]), ('Mattapan', [0, 0, 348, 627]), ('Harbor Islands', [0, 0, 0, 155]), ('Brighton', [0, 0, 983, 1466]), ('South Boston', [0, 1, 410, 1061]), ('West End', [0, 5, 387, 549]), ('Fenway', [4, 21, 893, 1034]), ('Chinatown', [11, 21, 74, 112]), ('Jamaica Plain', [0, 0, 356, 919])]
        scores =  repo.francisz_jrashaan.neighborhoodScores.find()
      
        scoreArray = []
        print(scores)
        for i in scores:
            print(i)
            a = lambda t: (t['Charging Station'])
            b = lambda t: (t['Hubway Stations'])
            c = lambda t: (t['Bike Networks'])  
            d = lambda t: (t['Open Space'])
            e = lambda t:(t['Neighborhood'])
           
    

 
            co1 = a(i)
            co2 = b(i)
            co3 = c(i)
            co4 = d(i)
            co5 = e(i)

            scoreArray.append((co1,co2,co3,co4,co5))
          

        S = z3.Solver()
        chargingStations = []
        hubwayStations = []
        bikeNetworks = []
        openspace = []
        #change .s in z3 printer z3 core and z3 
        for i in range(len(scoreArray)):

            c = scoreArray[i][0] 
            h = scoreArray[i][1]
            b = scoreArray[i][2]
            o = scoreArray[i][3]
            n = scoreArray[i][4]
            (x1,x2,x3,x4) = [z3.Real('x'+str(j) + "_" + str(i)) for j in range(1,5)]
            S.add(x1 > 10)
            S.add(x2 > 20)
            S.add(x3 > 15)
            S.add(x4 > 5)




            chargingStations.append(x1)
            hubwayStations.append(x2)
            bikeNetworks.append(x3)
            openspace.append(x4)

            S.add(((c+x1) * 1000) + ((h+x2) * 2000) + ((b+x3) * 1500) + ((o+x4) * 10) <= 5000000)
            chargingStations.append(c+x1)
            hubwayStations.append(x2)
            bikeNetworks.append(x3)
            openspace.append(x4)

        print(chargingStations)
        print("STOP")
        S.add(sum(chargingStations) > 100)
        S.add(sum(hubwayStations) > 150)
        S.add(sum(bikeNetworks) > 100)
        S.add(sum(openspace) > 50)




        S.check()
        print(S.model())
        t = S.model()
        print(t[0][1])
        print(t[1])





        
    
        

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
        
        
        this_script = doc.agent('alg:francisz_jrashaan#BudgetCalculator', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_neighborhoodscores = doc.entity('dat:francisz_jrashaan#neighborhoodScores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
        
        compute_budget = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        

        doc.wasAssociatedWith(compute_budget, this_script)
       
        doc.usage(compute_budget, resource_neighborhoodscores, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
     
                  
        optimalscore = doc.entity('dat:francisz_jrashaan#optimalScore', {prov.model.PROV_LABEL:'Correlation Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optimalscore, this_script)
        doc.wasGeneratedBy(optimalscore, compute_budget, endTime)
        doc.wasDerivedFrom(optimalscore, resource_neighborhoodscores, compute_budget, compute_budget, compute_budget)
                  
        repo.logout()
                  
        return doc
BudgetCalculator.execute()
doc = BudgetCalculator.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


