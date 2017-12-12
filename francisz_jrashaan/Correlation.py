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
    
    reads = ['francisz_jrashaan.neighborhoodscores']
    
    writes = ['francisz_jrashaan.correlationScore']
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')
        repo.dropCollection("correlationScore")
        repo.createCollection("correlationScore")
        scores =  repo.francisz_jrashaan.neighborhoodscores.find()
        
        scoreArray = []
        for x in scores:
           scoreArray.append(x)


        #print(scoreArray[1]['Charging Station'])
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
        score.append(("correlation between hubway stations & bikenetworks",scipy.stats.pearsonr(x4,y4)))
        score.append(("correlation between hubway stations & openspaces",scipy.stats.pearsonr(x5,y5)))
        score.append(("correlation between bikenetworks & openspaces",scipy.stats.pearsonr(x6,y6)))
        print(score)
        fixedScore=[]
        for x in score:
            y = lambda t: ({t[0]: t[1]})
            z = y(x)
            fixedScore.append(z)
        
        
             
    
#print(fixedScore)

        repo['francisz_jrashaan.correlationScore'].insert_many(fixedScore)
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
        
        
        this_script = doc.agent('alg:francisz_jrashaan#Correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhoodscores = doc.entity('dat:francisz_jrashaan#neighborhoodscores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
        
        compute_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        

        doc.wasAssociatedWith(compute_correlation, this_script)
       
        doc.usage(compute_correlation, neighborhoodscores, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
     
                  
        correlationScore = doc.entity('dat:francisz_jrashaan#correlationScore', {prov.model.PROV_LABEL:'Correlation Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlationScore, this_script)
        doc.wasGeneratedBy(correlationScore, compute_correlation, endTime)
        doc.wasDerivedFrom(correlationScore, neighborhoodscores, compute_correlation, compute_correlation, compute_correlation)
                  
        repo.logout()
                  
        return doc

Correlation.execute()
doc = Correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


