import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class zipcode_info(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = ["gaudiosi_raykatz.zipcode_map", "gaudiosi_raykatz.demographic_percentages", "gaudiosi_raykatz.housing_percentages", "gaudiosi_raykatz.income_percentages", "gaudiosi_raykatz.mbta_stops"]
    writes = ['gaudiosi_raykatz.zipcode_info']

    @staticmethod
    def execute(trial = False):
        '''Merge zipcode info'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
       
        r = []
        
        zipcode_data = list(repo.gaudiosi_raykatz.zipcode_map.find({}))[0]

        zipcode_list = []
        for feature in zipcode_data["features"]:
            zipcode_list.append(feature['properties']['ZIP5'])
        
        zipcode_list = list(set(zipcode_list))
        
        for zipcode in zipcode_list:
            z = {}
            z["zipcode"] = zipcode
            demographics = list(repo.gaudiosi_raykatz.demographic_percentages.find({"zipcode": zipcode}))
            if len(demographics) == 0:
                continue
            else:
               demographics = demographics[0]
            income = list(repo.gaudiosi_raykatz.income_percentages.find({"zipcode": zipcode}))[0]
            housing = list(repo.gaudiosi_raykatz.housing_percentages.find({"zipcode": zipcode}))[0]
            
            
            z["percent_white"] = demographics["percent_white"]
            z["percent_black"] = demographics["percent_black"]
            z["percent_native"] = demographics["percent_native"]
            z["percent_asian"] = demographics["percent_asian"]
            z["percent_pacific"] = demographics["percent_pacific"]
            z["percent_hispanic"] = demographics["percent_hispanic"]

            z["percent_married_households"] = demographics["percent_married_households"]
            z["percent_unemployed"] = demographics["percent_unemployed"]
            z["percent_in_labor_force"] = demographics["percent_in_labor_force"]
            z["percent_public_transit"] = demographics["percent_public_transit"]

            z["median_income"] = income["median_income"]
            z["median_rent"] = income["median_rent"]
            z["percent_spending_50_rent"] = income["percent_spending_50_rent"]
            z["percent_poverty"] = income["percent_poverty"]
            
            z["percent_homes_occupied"] = housing["percent_homes_occupied"]
            z["percent_homes_vacant"] = housing["percent_homes_vacant"]
            z["percent_homes_built_before_1939"] = housing["percent_homes_built_before_1939"]

            z["subway_stops"] = repo.gaudiosi_raykatz.mbta_stops.count({"zipcode": zipcode, "mode_name": "Subway"})
            z["commuter_stops"] = repo.gaudiosi_raykatz.mbta_stops.count({"zipcode": zipcode, "mode_name": "Commuter Rail"})
            z["bus_stops"] = repo.gaudiosi_raykatz.mbta_stops.count({"zipcode": zipcode, "mode_name": "Bus"})
            
            
            r.append(z)
            
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("zipcode_info")
        repo.createCollection("zipcode_info")
        repo['gaudiosi_raykatz.zipcode_info'].insert_many(r)
        repo['gaudiosi_raykatz.zipcode_info'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.zipcode_info'].metadata())
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
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gaudiosi_raykatz#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Zipcode Info&$select=zipcode,percent_white,percent_black,percent_native,percent_asian,percent_pacific,percent_hispanic,median_income,median_rent,percent_spending_50_rent,percent_poverty,percent_homes_occupied,percent_homes_vacant,percent_homes_built_before_1939,subway_stops,commuter_stops,bus_stops'
                  }
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz#zipcode_info', {prov.model.PROV_LABEL:'Demographics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
zipcode_info.execute()
doc = zipcode_info.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
