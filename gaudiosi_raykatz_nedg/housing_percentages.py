import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class housing_percentages(dml.Algorithm):
    contributor = 'gaudiosi_raykatz_nedg'
    reads = ["gaudiosi_raykatz_nedg.housing"]
    writes = ['gaudiosi_raykatz_nedg.housing_percentages']

    @staticmethod
    def execute(trial = False):
        '''Merge zipcode info'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
       
        
        repo.dropCollection("housing_percentages")
        repo.createCollection("housing_percentages")

        repo.gaudiosi_raykatz_nedg.housing.aggregate( [ {"$project":{
                                                "zipcode":1,
                                                "percent_homes_occupied":{"$divide": ["$occupied_housing", "$total_housing"]},
                                                "percent_homes_vacant":{"$divide": ["$vacant_housing", "$total_housing"]},
                                                "percent_homes_built_before_1939":{"$divide": ["$structures_built_before_1939", "$total_structures_built"]},
                                                "percent_renting":{"$divide": ["$renter_occupied", "$total_occupied"]},
                                                }},
                                                
                                                {"$out": "gaudiosi_raykatz_nedg.housing_percentages"}

        ])
         
        repo['gaudiosi_raykatz_nedg.housing_percentages'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz_nedg.housing_percentages'].metadata())
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gaudiosi_raykatz_nedg#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:gaudiosi_raykatz_nedg#housing', {'prov:label':'Housing', prov.model.PROV_TYPE:'ont:DataSet'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz_nedg#housing_percentages', {prov.model.PROV_LABEL:'Housing Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
housing_percentages.execute()
doc = housing_percentages.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
