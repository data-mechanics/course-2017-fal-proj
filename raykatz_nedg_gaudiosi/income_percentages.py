import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class income_percentages(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = ["raykatz_nedg_gaudiosi.income"]
    writes = ['raykatz_nedg_gaudiosi.income_percentages']

    @staticmethod
    def execute(trial = False):
        '''Get income percentages'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
       
        
        repo.dropCollection("income_percentages")
        repo.createCollection("income_percentages")
        
        repo.raykatz_nedg_gaudiosi.income.aggregate( [ {"$project":{
                                                "zipcode":1,
                                                "median_income":1,
                                                "median_rent":1,
                                                "percent_spending_50_rent":{"$divide": ["$50_income_rent", "$total_renters"]},
                                                "percent_poverty":{"$divide": ["$people_in_poverty", "$total_people"]},
                                                }},
                                                
                                                {"$out": "raykatz_nedg_gaudiosi.income_percentages"}

                                                
        ])
 
         
        repo['raykatz_nedg_gaudiosi.income_percentages'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.income_percentages'].metadata())
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
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:raykatz_nedg_gaudiosi#income', {'prov:label':'Income', prov.model.PROV_TYPE:'ont:DataSet'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )
        
        demos = doc.entity('dat:raykatz_nedg_gaudiosi#income_percentages', {prov.model.PROV_LABEL:'Income Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
income_percentages.execute()
doc = income_percentages.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
