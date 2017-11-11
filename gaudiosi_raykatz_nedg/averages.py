import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class averages(dml.Algorithm):
    contributor = 'gaudiosi_raykatz_nedg'
    reads = ["gaudiosi_raykatz_nedg.zipcode_info"]
    writes = ['gaudiosi_raykatz_nedg.averages']

    @staticmethod
    def execute(trial = False):
        '''Computes averages for the city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
       
        
        repo.dropCollection("averages")
        repo.createCollection("averages")

        repo.gaudiosi_raykatz_nedg.zipcode_info.aggregate( [
            {   
                "$group": {
                    "_id": "null",
                    "avg_percent_white": { "$avg": "$percent_white" },
                    "avg_percent_black": { "$avg": "$percent_black" },
                    "avg_percent_native": { "$avg": "$percent_native" },
                    "avg_percent_asian": { "$avg": "$percent_asian" },
                    "avg_percent_pacific": { "$avg": "$percent_pacific" },
                    "avg_percent_hispanic": { "$avg": "$percent_hispanic" },
                    "avg_percent_married_households": { "$avg": "$percent_married_households" },
                    "avg_percent_unemployed": { "$avg": "$percent_unemployed" },
                    "avg_percent_in_labor_force": { "$avg": "$percent_in_labor_force" },
                    "avg_percent_public_transit": { "$avg": "$percent_public_transit" },
                    "avg_median_income": { "$avg": "$median_income" },
                    "avg_median_rent": { "$avg": "$median_rent" },
                    "avg_percent_spending_50_rent": { "$avg": "$percent_spending_50_rent" },
                    "avg_percent_poverty": { "$avg": "$percent_poverty" },
                    "avg_percent_homes_occupied": { "$avg": "$percent_percent_homes_occupied" },
                    "avg_percent_homes_vacant": { "$avg": "$percent_homes_vacant" },
                    "avg_percent_homes_built_before_1939": { "$avg": "$percent_homes_built_before_1939" },
                    "avg_percent_renting": { "$avg": "$percent_renting" },
                    "avg_subway_stops": { "$avg": "$subway_stops" },
                    "avg_commuter_stops": { "$avg": "$commuter_stops" },
                    "avg_bus_stops": { "$avg": "$bus_stops" },
                }
            },{
                "$out": "gaudiosi_raykatz_nedg.averages",
            }
        ])

        

        repo['gaudiosi_raykatz_nedg.averages'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz_nedg.averages'].metadata())
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
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Gentrification Score&$select=zipcode,score'
                  }
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz_nedg#averages', {prov.model.PROV_LABEL:'Averages', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
averages.execute()
doc = averages.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
