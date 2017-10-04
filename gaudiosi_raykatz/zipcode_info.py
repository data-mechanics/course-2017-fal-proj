import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class zipcode_info(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = ["gaudiosi_katz.zipcode_map", "gaudiosi_katz.demographics", "gaudiosi_katz.housing", "gaudiosi_katz.income", "gaudiosi_katz.mbta_stops"]
    writes = ['gaudiosi_katz.zipcode_info']

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
        demographics = list(repo.gaudiosi_raykatz.demographics.find({}))
        housing = list(repo.gaudiosi_raykatz.housing.find({}))
        income = list(repo.gaudiosi_raykatz.income.find({}))
        mbta_stops = list(repo.gaudiosi_raykatz.mbta_stops.find({}))
        
        for zipcode in zipcode_list:
            z = {}
            z["zipcode"] = zipcode
            demographics = list(repo.gaudiosi_raykatz.demographics.find({"zipcode": zipcode}))[0]
            housing = list(repo.gaudiosi_raykatz.housing.find({"zipcode": zipcode}))[0]
            income = list(repo.gaudiosi_raykatz.income.find({"zipcode": zipcode}))[0]
            if demographics["total"] == "0":
                continue

            z["percent_white"] = "{0:.4f}".format( float(demographics["white"]) / float(demographics["total"]) )
            z["percent_black"] = "{0:.4f}".format( float(demographics["black"]) / float(demographics["total"]) )
            z["percent_native"] = "{0:.4f}".format( float(demographics["native_american"]) / float(demographics["total"]) )
            z["percent_asian"] = "{0:.4f}".format( float(demographics["asian"]) / float(demographics["total"]) )
            z["percent_pacific"] = "{0:.4f}".format( float(demographics["pacific_islander"]) / float(demographics["total"]) )
            z["percent_hispanic"] = "{0:.4f}".format( float(demographics["hispanic"]) / float(demographics["total"]) )
            z["median_income"] = income["median_income"]
            z["median_rent"] = income["median_rent"]
            z["percent_spending_50_rent"] = "{0:.4f}".format( float(income["50_income_rent"]) / float(income["total_renters"]) )
            z["percent_poverty"] = "{0:.4f}".format( float(income["people_in_poverty"]) / float(income["total_people"]) )
            z["percent_homes_occupied"] = "{0:.4f}".format( float(housing["occupied_housing"]) / float(housing["total_housing"]) )
            z["percent_homes_vacant"] = "{0:.4f}".format( float(housing["vacant_housing"]) / float(housing["total_housing"]) )
            z["perecent_homes_built_before_1939"] = "{0:.4f}".format( float(housing["structures_built_before_1939"]) / float(housing["total_structures_built"]) )
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
                  'ont:Query':'?type=Demographics&$select=white,black,native_american,asian,pacific_islander,hispanic,total,zipcode'
                  }
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz#zipcode_info', {prov.model.PROV_LABEL:'Demographics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc

zipcode_info.execute()
doc = zipcode_info.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
