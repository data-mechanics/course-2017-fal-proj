import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import geojson
import scipy.stats

#from tqdm import tqdm
import pdb
from random import shuffle
from math import sqrt

class Correlation(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    
    reads = ['francisz_jrashaan.neighborhoodScores']
    
    writes = ['francisz_jrashaan.correlationScore']
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')
        repo.dropCollection("correlationScore")
        repo.createCollection("correlationScore")
        scores =  repo.francisz_jrashaan.neighborhoodScores.find()
      
        scoreArray = []
        for x in scores:
            scoreArray.append(x)


        print(scoreArray[1]['Charging Station'])
        print("TESTING")
        #scores = [('North End', [0, 3, 236, 240]), ('Bay Village', [0, 0, 24, 42]), ('East Boston', [0, 19, 222, 3544]), ('Leather District', [8, 8, 34, 43]), ('Allston', [0, 1, 1888, 1994]), ('Hyde Park', [0, 0, 569, 1163]), ('Roslindale', [0, 0, 450, 608]), ('Charlestown', [0, 7, 189, 455]), ('Back Bay', [4, 17, 432, 817]), ('South End', [0, 0, 116, 150]), ('Downtown', [4, 33, 160, 420]), ('Dorchester', [0, 7, 1382, 3710]), ('South Boston Waterfront', [15, 7, 102, 222]), ('West Roxbury', [0, 0, 559, 708]), ('Longwood Medical Area', [0, 11, 136, 154]), ('Mission Hill', [0, 11, 135, 161]), ('Roxbury', [0, 7, 315, 525]), ('Beacon Hill', [1, 16, 149, 391]), ('Mattapan', [0, 0, 348, 627]), ('Harbor Islands', [0, 0, 0, 155]), ('Brighton', [0, 0, 983, 1466]), ('South Boston', [0, 1, 410, 1061]), ('West End', [0, 5, 387, 549]), ('Fenway', [4, 21, 893, 1034]), ('Chinatown', [11, 21, 74, 112]), ('Jamaica Plain', [0, 0, 356, 919])]
        relationdata1 = []
        relationdata2 = []
        relationdata3 = []
        relationdata4 = []
        relationdata5 = []
        relationdata6 = []
      

        
        Correlations = []

        for i in scoreArray:
            a = lambda t: ((t['Charging Station'], t['Hubway Stations']))
            b = lambda t: ((t['Charging Station'], t['Bike Networks']))    
            c = lambda t: ((t['Charging Station'], t['Open Space']))     
            d = lambda t: ((t['Hubway Stations'], t['Bike Networks'])) 
            e = lambda t: ((t['Hubway Stations'], t['Open Space']))
            f = lambda t: ((t['Bike Networks'], t['Open Space']))  
           
    

 
            co1 = a(i)
            co2 = b(i)
            co3 = c(i)
            co4 = d(i)
            co5 = e(i)
            co6 = f(i)

            relationdata1.append(co1)
            relationdata2.append(co2)
            relationdata3.append(co3)
            relationdata4.append(co4)
            relationdata5.append(co5)
            relationdata6.append(co6)



        x1 = [xi for (xi, yi) in relationdata1]
        y1 = [yi for (xi, yi) in relationdata1]
        x2 = [xi for (xi, yi) in relationdata2]
        y2 = [yi for (xi, yi) in relationdata2]
        x3 = [xi for (xi, yi) in relationdata3]
        y3 = [yi for (xi, yi) in relationdata3]
        x4 = [xi for (xi, yi) in relationdata4]
        y4 = [yi for (xi, yi) in relationdata4]
        x5 = [xi for (xi, yi) in relationdata5]
        y5 = [yi for (xi, yi) in relationdata5]
        x6 = [xi for (xi, yi) in relationdata6]
        y6 = [yi for (xi, yi) in relationdata6]
        



    




        score = []
        score.append(("correlation between charging stations & hubway stations ",scipy.stats.pearsonr(x1,y1)))
        score.append(("correlation between charging stations & bikenetworks",scipy.stats.pearsonr(x2,y2)))
        score.append(("correlation between charging stations & openspaces",scipy.stats.pearsonr(x3,y3)))
        score.append(("correlation between hubway stations   & bikenetworks",scipy.stats.pearsonr(x4,y4)))
        score.append(("correlation between hubway stations & openspaces",scipy.stats.pearsonr(x5,y5)))
        score.append(("correlation between bikenetworks & openspaces",scipy.stats.pearsonr(x6,y6)))

        fixedScore= []
        for x in score:
             print(x)
             y = lambda t: ({t[0],t[1]})
             z = y(x)
             fixedScore.append(z)
             
    
        print(fixedScore)



        #repo['francisz_jrashaan.correlationScore'].insert_many(fixedScore)
        #repo['francisz_jrashaan.correlationScore'].metadata({'complete':True})
        


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
        
        
        this_script = doc.agent('alg:francisz_jrashaan#Correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_neighborhoodscores = doc.entity('dat:francisz_jrashaan#neighborhoodScores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
        
        compute_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        

        doc.wasAssociatedWith(compute_correlation, this_script)
       
        doc.usage(compute_correlation, resource_neighborhoodscores, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
     
                  
        correlationscore = doc.entity('dat:francisz_jrashaan#Correlation', {prov.model.PROV_LABEL:'Correlation Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlationscore, this_script)
        doc.wasGeneratedBy(correlationscore, compute_correlation, endTime)
        doc.wasDerivedFrom(correlationscore, resource_neighborhoodscores, compute_correlation, compute_correlation, compute_correlation)
                  
        repo.logout()
                  
        return doc

Correlation.execute()
doc = Correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


