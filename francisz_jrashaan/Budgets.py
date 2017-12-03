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
#import z3core
#import z3types
#import z3consts
#import z3printer
import random
from math import ceil
    
class Budgets(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    
    reads = ['francisz_jrashaan.neighborhoodScores']
    
    writes = ['francisz_jrashaan.optimalScore']
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')
        scores =  repo.francisz_jrashaan.neighborhoodScores.find()
        scoreArray = []
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
        results = []
        budget = 0
        multiplier = 0
        for i in range(25):
            chargingStations = []
            hubwayStations = []
            bikeNetworks = []
            openspace = []
            Neighborhoods = []
            budget += 100000
            multiplier += 1
            for i in range(len(scoreArray)):
                c = scoreArray[i][0]
                h = scoreArray[i][1]
                b = scoreArray[i][2]
                o = scoreArray[i][3]
                Neighborhoods += [scoreArray[i][4]]
                (x1,x2,x3,x4) = [z3.Real('x'+str(j) + "_" + str(i)) for j in range(1,5)]
                S.add(x1 > 1)
                S.add(x2 > 1)
                S.add(x3 > 1)
                S.add(x4 > 1)
                #S.add(x1 < budget/2000)
                #S.add(x2 < budget/1500)
                #S.add(x3 < budget/3000)
                #S.add(x4 < budget/8000)
                L = ceil(random.randint(10, random.randint(15, 100))//10 * multiplier)
                S.add(x1 >= L)
                print("Budget: " + str(budget) + " " +  "Chargingstations: " + str(L))
                L = ceil(random.randint(10, random.randint(15, 100))//10 * multiplier)
                S.add(x2 >= L)
                print("Budget: " + str(budget) + " " +  "Hubwaystations: " + str(L))
                L = ceil(random.randint(10, random.randint(15, 100))//10 * multiplier)
                S.add(x3 >= L)
                print("Budget: " + str(budget) + " " +  "Bikeroutes: " + str(L))
                L = ceil(random.randint(10, random.randint(15, 40))//10 * multiplier)
                S.add(x4 >= L)
                print("Budget: " + str(budget) + " " +  "Openspace: " + str(L))
                S.add(((x1) * 2000) + ((x2) * 1500) + ((x3) * 3000) + ((x4) * 8000) <= budget * 20)
                print(S.check())
        
            #print(S.check())
            if(str(S.check()) != "unsat"):
                X = S.model()
                for i in range(len(scoreArray)):
                    (x1,x2,x3,x4) = [z3.Real('x'+str(j) + "_" + str(i)) for j in range(1,5)]
                    chargingStations.append(X[x1])
                    hubwayStations.append(X[x2])
                    bikeNetworks.append(X[x3])
                    openspace.append(X[x4])

                    for j in range(len(chargingStations)):
                        tuple = (Neighborhoods[j], chargingStations[j], hubwayStations[j], bikeNetworks[j], openspace[j])
                        results.append(("Budget: " + str(budget), tuple))

        print(results)

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
        
        this_script = doc.agent('alg:francisz_jrashaan#Budgets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_Budgets = doc.entity('dat:francisz_jrashaan#Budgets', {'prov:label':'Budgets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
        
        compute_Budgets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        
        doc.wasAssociatedWith(compute_Budgets, this_script)
        
        doc.usage(compute_Budgets, resource_Budgets, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
        
        optimalscores = doc.entity('dat:francisz_jrashaan#optimalscores', {prov.model.PROV_LABEL:'optimalscores', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optimalscores, this_script)
        doc.wasGeneratedBy(optimalscores, compute_Budgets, endTime)
        doc.wasDerivedFrom(optimalscores, resource_Budgets, compute_Budgets, compute_Budgets, compute_Budgets)
        
        repo.logout()
        
        return doc

Budgets.execute()
doc = Budgets.provenance()
