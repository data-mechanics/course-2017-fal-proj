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

        #scores = [('North End', [0, 3, 236, 240]), ('Bay Village', [0, 0, 24, 42]), ('East Boston', [0, 19, 222, 3544]), ('Leather District', [8, 8, 34, 43]), ('Allston', [0, 1, 1888, 1994]), ('Hyde Park', [0, 0, 569, 1163]), ('Roslindale', [0, 0, 450, 608]), ('Charlestown', [0, 7, 189, 455]), ('Back Bay', [4, 17, 432, 817]), ('South End', [0, 0, 116, 150]), ('Downtown', [4, 33, 160, 420]), ('Dorchester', [0, 7, 1382, 3710]), ('South Boston Waterfront', [15, 7, 102, 222]), ('West Roxbury', [0, 0, 559, 708]), ('Longwood Medical Area', [0, 11, 136, 154]), ('Mission Hill', [0, 11, 135, 161]), ('Roxbury', [0, 7, 315, 525]), ('Beacon Hill', [1, 16, 149, 391]), ('Mattapan', [0, 0, 348, 627]), ('Harbor Islands', [0, 0, 0, 155]), ('Brighton', [0, 0, 983, 1466]), ('South Boston', [0, 1, 410, 1061]), ('West End', [0, 5, 387, 549]), ('Fenway', [4, 21, 893, 1034]), ('Chinatown', [11, 21, 74, 112]), ('Jamaica Plain', [0, 0, 356, 919])]
        relationdata1 = []
        relationdata2 = []
        relationdata3 = []
        relationdata4 = []
        relationdata5 = []
        relationdata6 = []
      


        Correlations = []

        for i in scores:
            a = lambda t: ((t[1][0], t[1][1]))
            b = lambda t: ((t[1][0], t[1][2]))    
            c = lambda t: ((t[1][0], t[1][3]))     
            d = lambda t: ((t[1][1], t[1][2])) 
            e = lambda t: ((t[1][1], t[1][3]))
            f = lambda t: ((t[1][2], t[1][3]))  
           
    

 
            co1 = a(i)
            co2 = b(i)
            co3 = c(i)
            co4 = d(i)
            co5 = e(i)
            co6 = f(i)

            relationdata1.append(co1)
            relationdata2.append(co2)
            relationdata3.append(co3)
            relationaldata4.append(co4)
            relationaldata5.append(co5)
            relationaldata6.append(co6)



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



    

    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def avg(x): # Average
        return sum(x)/len(x)

    def stddev(x): # Standard deviation.
        m = avg(x)
        return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

    def cov(x, y): # Covariance.
        return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

    def corr(x, y): # Correlation coefficient.
        if stddev(x)*stddev(y) != 0:
            return cov(x, y)/(stddev(x)*stddev(y))

    def p(x, y):
        c0 = corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = permute(y)
            corrs.append(corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)
    score = []
    score.append(("charging station & hubway",p(x1,y1)))
    score.append(("charging station & bikenetwork",p(x2,y2)))
    score.append(("charging station & openspace",p(x3,y3)))
    score.append(("hubway & bikenetwork",p(x4,y4)))
    score.append(("hubway & openspace",p(x5,y5)))
    score.append(("bikenetwork & openspace",p(x6,y6)))





    repo['francisz_jrashaan.correlationScore'].insert_many(score)
    repo['francisz_jrashaan.correlationScore'].metadata({'complete':True})



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
        
        
        this_script = doc.agent('alg:francisz_jrashaan#correlationScore', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_neighborhoodscores = doc.entity('dat:francisz_jrashaan#NeighborhoodScores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
        
        compute_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        

        doc.wasAssociatedWith(compute_correlation, this_script)
       
        doc.usage(compute_correlation, resource_neighborhood, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
     
                  
        correlationscore = doc.entity('dat:francisz_jrashaan#correlationScore', {prov.model.PROV_LABEL:'Correlation Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlationScore, this_script)
        doc.wasGeneratedBy(correlationScore, compute_correlation, endTime)
        doc.wasDerivedFrom(correlationScore, resource_neighborhood, compute_correlation, compute_correlation, compute_correlation)
                  
        repo.logout()
                  
        return doc

correlation.execute()
doc = correlation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


