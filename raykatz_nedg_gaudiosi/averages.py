import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class averages(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = ["raykatz_nedg_gaudiosi.zipcode_info"]
    writes = ['raykatz_nedg_gaudiosi.averages']

    @staticmethod
    def execute(trial = False):
        '''Computes averages for the city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
       
        
        repo.dropCollection("averages")
        repo.createCollection("averages")

        repo.raykatz_nedg_gaudiosi.zipcode_info.aggregate( [
            {   
                "$group": {
                    "_id": "null",
                    "avg_percent_white": { "$avg": "$percent_white" },
                    "std_percent_white": { "$stdDevPop": "$percent_white"},
                    "avg_percent_black": { "$avg": "$percent_black" },
                    "std_percent_black": { "$stdDevPop": "$percent_black" },
                    "avg_percent_native": { "$avg": "$percent_native" },
                    "std_percent_native": { "$stdDevPop": "$percent_native" },
                    "avg_percent_asian": { "$avg": "$percent_asian" },
                    "std_percent_asian": { "$stdDevPop": "$percent_asian" },
                    "avg_percent_pacific": { "$avg": "$percent_pacific" },
                    "std_percent_pacific": { "$stdDevPop": "$percent_pacific" },
                    "avg_percent_hispanic": { "$avg": "$percent_hispanic" },
                    "std_percent_hispanic": { "$stdDevPop": "$percent_hispanic" },
                    "avg_percent_married_households": { "$avg": "$percent_married_households" },
                    "std_percent_married_households": { "$stdDevPop": "$percent_married_households" },
                    "avg_percent_unemployed": { "$avg": "$percent_unemployed" },
                    "std_percent_unemployed": { "$stdDevPop": "$percent_unemployed" },
                    "avg_percent_in_labor_force": { "$avg": "$percent_in_labor_force" },
                    "std_percent_in_labor_force": { "$stdDevPop": "$percent_in_labor_force" },
                    "avg_percent_public_transit": { "$avg": "$percent_public_transit" },
                    "std_percent_public_transit": { "$stdDevPop": "$percent_public_transit" },
                    "avg_median_income": { "$avg": "$median_income" },
                    "std_median_income": { "$stdDevPop": "$median_income" },
                    "avg_median_rent": { "$avg": "$median_rent" },
                    "std_median_rent": { "$stdDevPop": "$median_rent" },
                    "avg_percent_spending_50_rent": { "$avg": "$percent_spending_50_rent" },
                    "std_percent_spending_50_rent": { "$stdDevPop": "$percent_spending_50_rent" },
                    "avg_percent_poverty": { "$avg": "$percent_poverty" },
                    "std_percent_poverty": { "$stdDevPop": "$percent_poverty" },
                    "avg_percent_homes_occupied": { "$avg": "$percent_percent_homes_occupied" },
                    "std_percent_homes_occupied": { "$stdDevPop": "$percent_percent_homes_occupied" },
                    "avg_percent_homes_vacant": { "$avg": "$percent_homes_vacant" },
                    "std_percent_homes_vacant": { "$stdDevPop": "$percent_homes_vacant" },
                    "avg_percent_homes_built_before_1939": { "$avg": "$percent_homes_built_before_1939" },
                    "std_percent_homes_built_before_1939": { "$stdDevPop": "$percent_homes_built_before_1939" },
                    "avg_percent_renting": { "$avg": "$percent_renting" },
                    "std_percent_renting": { "$stdDevPop": "$percent_renting" },
                    "avg_subway_stops": { "$avg": "$subway_stops" },
                    "std_subway_stops": { "$stdDevPop": "$subway_stops" },
                    "avg_commuter_stops": { "$avg": "$commuter_stops" },
                    "std_commuter_stops": { "$stdDevPop": "$commuter_stops" },
                    "avg_bus_stops": { "$avg": "$bus_stops" },
                    "std_bus_stops": { "$stdDevPop": "$bus_stops" },
                }
            },{
                "$out": "raykatz_nedg_gaudiosi.averages",
            }
        ])

        

        repo['raykatz_nedg_gaudiosi.averages'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.averages'].metadata())
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

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_info', {'prov:label':'Zipcode Info', prov.model.PROV_TYPE:'ont:DataSet'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )
        
        demos = doc.entity('dat:raykatz_nedg_gaudiosi#averages', {prov.model.PROV_LABEL:'Averages', prov.model.PROV_TYPE:'ont:DataSet'})
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
