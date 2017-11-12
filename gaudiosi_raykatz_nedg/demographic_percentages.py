import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class demographic_percentages(dml.Algorithm):
    contributor = 'gaudiosi_raykatz_nedg'
    reads = ["gaudiosi_raykatz_nedg.demographics"]
    writes = ['gaudiosi_raykatz_nedg.demographic_percentages']

    @staticmethod
    def execute(trial = False):
        '''Get demographic percentages'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
       
        
        
        repo.dropCollection("demographic_percentages")
        repo.createCollection("demographic_percentages")
        
        
        repo.gaudiosi_raykatz_nedg.demographics.aggregate( [ {"$project":{
            "zipcode":1, 
            "percent_white":{"$divide": ["$white", "$total"]},
            "percent_black":{"$divide": ["$black", "$total"]},
            "percent_native":{"$divide": ["$native", "$total"]},
            "percent_asian":{"$divide": ["$asian", "$total"]},
            "percent_pacific":{"$divide": ["$pacific", "$total"]},
            "percent_hispanic":{"$divide": ["$hispanic", "$total"]},
            "percent_married_households":{"$divide": ["$married_households", "$total_households"]},
            "percent_unemployed":{"$divide": ["$unemployed","$labor_force"]},
            "percent_in_labor_force":{"$divide": ["$labor_force","$employment_count"]},
            "percent_public_transit":{"$divide": ["$public_transit", "$total_transit"]},
            }},
                                                
            {"$out": "gaudiosi_raykatz_nedg.demographic_percentages"}

                                                
        ])
        
        repo['gaudiosi_raykatz_nedg.demographic_percentages'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz_nedg.demographic_percentages'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:gaudiosi_raykatz_nedg#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:gaudiosi_raykatz_nedg#demographics', {'prov:label':'Demographics', prov.model.PROV_TYPE:'ont:DataSet'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz_nedg#demographic_percentages', {prov.model.PROV_LABEL:'Demographic Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
demographic_percentages.execute()
doc = demographic_percentages.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
