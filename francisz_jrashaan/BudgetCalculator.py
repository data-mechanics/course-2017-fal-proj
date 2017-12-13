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
import scipy.stats
import z3
import z3core
import z3types
import z3consts
import z3printer
import random

class BudgetCalculator(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    
    reads = ['francisz_jrashaan.neighborhoodScores']
    
    writes = ['francisz_jrashaan.optimalScore']
    
    
    @staticmethod
    def execute(trial = True):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')
        scores =  repo.francisz_jrashaan.neighborhoodScores.find()
        
        scoreArray = []
        #print(scores)
        for i in scores:
            #print(i)
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
        Neighborhoods = []
        results = []
        #change .s in z3 printer z3 core and z3
        for i in range(len(scoreArray)):
            c = scoreArray[i][0]
            h = scoreArray[i][1]
            b = scoreArray[i][2]
            o = scoreArray[i][3]
            Neighborhoods += [scoreArray[i][4]]
            (x1,x2,x3,x4) = [z3.Real('x'+str(j) + "_" + str(i)) for j in range(1,5)]
            S.add(x1 > 0)
            S.add(x2 > 0)
            S.add(x3 > 0)
            S.add(x4 > 0)
            S.add(x1 >= x2 + random.randint(4, 20))
            S.add(x2 >= random.randint(0, 10))
            S.add(x3 > x1 + random.randint(1, 20))
            S.add(x4 >= x1 + random.randint(1, 6))
            S.add(((x1) * 2000) + ((x2) * 1500) + ((x3) * 3000) + ((x4) * 8000) <= 1000000)

        S.check()
        X = S.model()
    #print(int(z3._to_int_str(X[7])))
        for i in range(len(scoreArray)):
            (x1,x2,x3,x4) = [z3.Real('x'+str(j) + "_" + str(i)) for j in range(1,5)]
            chargingStations.append(X[x1])
            hubwayStations.append(X[x2])
            bikeNetworks.append(X[x3])
            openspace.append(X[x4])
            
            #print(chargingStations)
            for j in range(len(chargingStations)):
                tuple = (Neighborhoods[j], chargingStations[j], hubwayStations[j], bikeNetworks[j], openspace[j])
                y = lambda t: ({"Neighborhood": t[0], 'Charging Station': str(t[1]), 'Hubway Stations': str(t[2]), 'Bike Networks': str(t[3]), 'Open Space': str(t[4])})
                if(tuple[1] > 250):
                    temp = tuple[1]
                    diff = tuple[1] - 250
                    score += 250 + diff*(.05)
                else:
                    score += tuple[1] * 1
                if(tuple[2] > 250):
                    temp = tuple[2]
                    diff = tuple[2] - 250
                    score += 250 + diff*(.05)
                else:
                    score += tuple[2] * 1
                if(tuple[3] > 250):
                    temp = tuple[3]
                    diff = tuple[3] - 250
                    score += 250 + diff*(.05)
                else:
                    score += tuple[3] * 1
                if(tuple[4] > 250):
                    temp = tuple[4]
                    diff = tuple[4] - 250
                    score += 250 + diff*(.05)
                else:
                    score += tuple[4] * 1
                results.append(("Budget: " + str(1000000), "Score: " + str(score), y(tuple)))
            
            #print(results)
            repo.dropCollection("optimalScore")
            repo.createCollection("optimalScore")
            repo['francisz_jrashaan.optimalScore'].insert_many(results)
            repo['francisz_jrashaan.optimalScore'].metadata({'complete':True})

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
        neighborhoodscores = doc.entity('dat:francisz_jrashaan#neighborhoodscores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
            
        compute_budget = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            
            
        doc.wasAssociatedWith(compute_budget, this_script)
            
        doc.usage(compute_budget,neighborhoodscores, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
            
        optimalScore = doc.entity('dat:francisz_jrashaan#optimalScore', {prov.model.PROV_LABEL:'Correlation Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optimalScore, this_script)
        doc.wasGeneratedBy(optimalScore, compute_budget, endTime)
        doc.wasDerivedFrom(optimalScore, neighborhoodscores, compute_budget, compute_budget, compute_budget)
            
        repo.logout()

        return doc
BudgetCalculator.execute()
doc = BudgetCalculator.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


