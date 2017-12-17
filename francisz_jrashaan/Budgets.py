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
from math import ceil
    
class Budgets(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    
    reads = ['francisz_jrashaan.presetneighborhoodScores']
    
    writes = ['francisz_jrashaan.budgets']
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')
        scores =  repo.francisz_jrashaan.presetneighborhoodScores.find()
        
      
        scoreArray = []
        average = [0, 0, 0, 0]
        count = 0
        for i in scores:
            #print(i)
            og_score = 0
            count += 1
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
            #average[0] += co1
            #average[1] += co2
            #average[2] += co3
            #average[3] += co4
            O_limit = 22.432692307692307
            if(co1 > O_limit):
                temp = co1
                diff = co1 - O_limit
                og_score += O_limit + diff*(.2)
            else:
                og_score += co1
            if(co2 > O_limit):
                temp = co2
                diff = co2 - O_limit
                og_score += O_limit + diff*(.2)
            else:
                og_score += co2
            if(co3 > O_limit):
                temp = co3
                diff = co3 - O_limit
                og_score += O_limit + diff*(.2)
            else:
                og_score += co3
            if(co4 > O_limit):
                temp = co4
                diff = co4 - O_limit
                og_score += O_limit + diff*(.2)
            else:
                og_score += co4
            scoreArray.append((co1,co2,co3,co4,co5,"Original Score: " + str(og_score/10)))
        #below lines used to calculate the original score for each neighborhood
        #average[0] = average[0]/count
        #average[1] = average[1]/count
        #average[2] = average[2]/count
        #average[3] = average[3]/count
        #Original_limit = (average[0] + average[1] + average[2] + average[3])/4
        #print(Original_limit)
        S = z3.Solver()
        results = []
        budget = 0
        multiplier = 0
        flag = 0
        #precalculated
        averages = [386.0673076923077, 416.96153846153845, 458.9038461538461, 519.625, 583.6057692307693, 637.0673076923077, 692.1346153846154, 762.8173076923076, 807.9230769230769, 880.7211538461538, 922.7788461538462, 992.5192307692307, 1049.6057692307693, 1134.6923076923078, 1178.2788461538462, 1269.201923076923, 1334.7115384615386, 1385.8749999999998, 1437.230769230769, 1536.1538461538464, 1597.6826923076924, 1654.5961538461538, 1718.0961538461538, 1776.6826923076924, 1842.7692307692305]
        for z in range(25):
            #print(len(averages))
            #averages = [0, 0, 0, 0]
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
                #add constraints
                #S.add(x1 > 1)
                #S.add(x2 > 1)
                #S.add(x3 > 1)
                #S.add(x4 > 1)
                #solutions are randomized, forced to be greater than a randomly defined limit. This was a precalculated measure.
                L = ceil(random.randint(10, random.randint(15, 100))//10 * multiplier)
                S.add(x1 >= L)
                L = ceil(random.randint(10, random.randint(15, 100))//10 * multiplier)
                S.add(x2 >= L)
                L = ceil(random.randint(10, random.randint(15, 100))//10 * multiplier)
                S.add(x3 >= L)
                L = ceil(random.randint(10, random.randint(15, 40))//10 * multiplier)
                S.add(x4 >= L)
                S.add(((x1) * 2000) + ((x2) * 1500) + ((x3) * 3000) + ((x4) * 8000) <= budget * 20)
        
            if(str(S.check()) != "unsat"):
                X = S.model()
                for i in range(len(scoreArray)):
                    (x1,x2,x3,x4) = [z3.Real('x'+str(j) + "_" + str(i)) for j in range(1,5)]
                    chargingStations.append(X[x1])
                    hubwayStations.append(X[x2])
                    bikeNetworks.append(X[x3])
                    openspace.append(X[x4])
                    new_score = 0
                    
                    tuple = ( str(chargingStations[i]), str(hubwayStations[i]), str(bikeNetworks[i]), str(openspace[i]))
                        #averages[0] += int(str(tuple[1])) + scoreArray[j][0]
                        #averages[1] += int(str(tuple[2])) + scoreArray[j][1]
                        #averages[2] += int(str(tuple[3])) + scoreArray[j][2]
                        #averages[3] += int(str(tuple[4])) + scoreArray[j][3]
                    if(int(str(tuple[0])) + scoreArray[i][0] > averages[z]):
                        temp = int(str(tuple[0]))
                        diff = int(str(tuple[0])) - averages[z]
                        new_score += averages[z] + diff*(.2) + scoreArray[i][0]
                    else:
                        new_score += int(str(tuple[0])) * 1 + scoreArray[i][0]
                    if(int(str(tuple[2])) + scoreArray[i][1]> averages[z]):
                        temp = int(str(tuple[1]))
                        diff = int(str(tuple[1])) - averages[z]
                        new_score += averages[z] + diff*(.2) + scoreArray[i][1]
                    else:
                        new_score += int(str(tuple[1])) * 1 + scoreArray[i][1]
                    if(int(str(tuple[2])) + scoreArray[i][2]> averages[z]):
                        temp = int(str(tuple[2]))
                        diff = int(str(tuple[2])) - averages[z]
                        new_score += averages[z] + diff*(.2) + scoreArray[i][2]
                    else:
                        new_score += int(str(tuple[2])) * 1 + scoreArray[i][2]
                    if(int(str(tuple[3])) + scoreArray[i][3]> averages[z]):
                        temp = int(str(tuple[3]))
                        diff = int(str(tuple[3])) - averages[z]
                        new_score += averages[z] + diff*(.2) + scoreArray[i][3]
                    else:
                        new_score += int(str(tuple[3])) * 1 + scoreArray[i][3]
                    
                    print(new_score)
                    if new_score > 1000: 
                        x = new_score / 100 * 3
                        if x > 100:
                            x = 100
                    else:
                        x =new_score / 10 * 3
                        if x > 100: 
                            x = 100




                    results.append({"Neighborhood":Neighborhoods[i],"Budget": budget, "New Score": (x) , "Originial Score":scoreArray[i][5], "Green Facilities": tuple})
                    
                            #if(score/10 > 100):
                            #flag += 1
            #The below lines defines the contents of the averages array
            #averages[0] = averages[0]/count
            #averages[1] = averages[1]/count
            #averages[2] = averages[2]/count
            #averages[3] = averages[3]/count
            #Averageitems = (averages[0] + averages[1] + averages[2] + averages[3])/4
            #print(Averageitems)
        print(results)
        repo.dropCollection("budgets")
        repo.createCollection("budgets")
        repo['francisz_jrashaan.budgets'].insert_many(results)
        repo['francisz_jrashaan.budgets'].metadata({'complete':True})
        #print(flag)

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
        presetneighborhoodScores = doc.entity('dat:francisz_jrashaan#presetneighborhoodScores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})

        
        compute_Budgets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        
        doc.wasAssociatedWith(compute_Budgets, this_script)
        
        doc.usage(compute_Budgets, presetneighborhoodScores, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
        
        budgets = doc.entity('dat:francisz_jrashaan#budgets', {'prov:label':'Budgets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'Dataset'})
        doc.wasAttributedTo(budgets, this_script)
        doc.wasGeneratedBy(budgets, compute_Budgets, endTime)
        doc.wasDerivedFrom(budgets, presetneighborhoodScores, compute_Budgets, compute_Budgets, compute_Budgets)
        
        repo.logout()
        
        return doc

Budgets.execute()
doc = Budgets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
