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
        
        
        this_script = doc.agent('alg:francisz_jrashaan#Correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_neighborhoodscores = doc.entity('dat:francisz_jrashaan#NeighborhoodScores', {'prov:label':'Neighborhood Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'BSON'})
        
        compute_budget = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        

        doc.wasAssociatedWith(compute_budget, this_script)
       
        doc.usage(compute_correlation, resource_neighborhood, startTime, None, {prov.model.PROV_TYPE:'ont:Used for Computation'})
     
                  
        optimalscore = doc.entity('dat:francisz_jrashaan#optimalScore', {prov.model.PROV_LABEL:'Correlation Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(optimalscore, this_script)
        doc.wasGeneratedBy(optimalscore, compute_budget, endTime)
        doc.wasDerivedFrom(optimalscore, resource_neighborhood, compute_budget, compute_budget, compute_budgets)
                  
        repo.logout()
                  
        return doc
BudgetCalculator.execute()
doc = constraint.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


