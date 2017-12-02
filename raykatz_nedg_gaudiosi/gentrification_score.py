import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class gentrification_score(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = ["raykatz_nedg_gaudiosi.zipcode_map","raykatz_nedg_gaudiosi.zipcode_info","raykatz_nedg_gaudiosi.averages"]
    writes = ['raykatz_nedg_gaudiosi.gentrification_score']

    @staticmethod
    def execute(trial = False):
        '''Creates a gentrification score for each neighborhood of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
       
        r = []
        

        # First, find the averages
        standardized = list(repo.raykatz_nedg_gaudiosi.averages.find({}))[0]

        # Then, compute the score for each one
        zipcode_data = list(repo.raykatz_nedg_gaudiosi.zipcode_map.find({}))[0]

        zipcode_list = []
        for feature in zipcode_data["features"]:
            zipcode_list.append(feature['properties']['ZIP5'])
        
        zipcode_list = list(set(zipcode_list))
        
        for zipcode in zipcode_list:
            z = {}
            z["zipcode"] = zipcode
            zip_info  = list(repo.raykatz_nedg_gaudiosi.zipcode_info.find({"zipcode": zipcode}))
            if len(zip_info) == 0:
                continue
            else:
                zip_info = zip_info[0]

            score = 0
            score += -1*((zip_info["percent_white"] - standardized["avg_percent_white"]) / standardized["std_percent_white"])
            score += -1*((zip_info["percent_married_households"] - standardized["avg_percent_married_households"]) / standardized["std_percent_married_households"])
            score += ((zip_info["percent_unemployed"] - standardized["avg_percent_unemployed"]) / standardized["std_percent_unemployed"])
            score += ((zip_info["percent_in_labor_force"] - standardized["avg_percent_in_labor_force"]) / standardized["std_percent_in_labor_force"])
            score += ((zip_info["percent_public_transit"] - standardized["avg_percent_public_transit"]) / standardized["std_percent_public_transit"])
            score += -1*((zip_info["median_income"] - standardized["avg_median_income"]) / standardized["std_median_income"])
            score += -1*((zip_info["median_rent"] - standardized["avg_median_rent"]) / standardized["std_median_rent"])
            score += ((zip_info["percent_spending_50_rent"] - standardized["avg_percent_spending_50_rent"]) / standardized["std_percent_spending_50_rent"])
            score += ((zip_info["percent_poverty"] - standardized["avg_percent_poverty"]) / standardized["std_percent_poverty"])
            score += ((zip_info["percent_homes_built_before_1939"] - standardized["avg_percent_homes_built_before_1939"]) / standardized["std_percent_homes_built_before_1939"])
            score += ((zip_info["percent_renting"] - standardized["avg_percent_renting"]) / standardized["std_percent_renting"])
            score += ((zip_info["subway_stops"] - standardized["avg_subway_stops"]) / standardized["std_subway_stops"])
            if not trial:
                score += ((zip_info["bus_stops"] - standardized["avg_bus_stops"]) / standardized["std_bus_stops"])
            
            z["score"] = score

            r.append(z)
            
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("gentrification_score")
        repo.createCollection("gentrification_score")
        repo['raykatz_nedg_gaudiosi.gentrification_score'].insert_many(r)
        repo['raykatz_nedg_gaudiosi.gentrification_score'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.gentrification_score'].metadata())
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
        resource =  doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_info', {'prov:label':'Zipcode Info', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_map', {'prov:label':'Zipcode Map', prov.model.PROV_TYPE:'ont:DataSet'})
        resource3 = doc.entity('dat:raykatz_nedg_gaudiosi#averages', {'prov:label':'Averages', prov.model.PROV_TYPE:'ont:DataSet'})

        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )

        doc.usage(get_demos, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )

        doc.usage(get_demos, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )

        demos = doc.entity('dat:raykatz_nedg_gaudiosi#gentrification_score', {prov.model.PROV_LABEL:'Gentrification Score', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)
        doc.wasDerivedFrom(demos, resource2, get_demos, get_demos, get_demos)
        doc.wasDerivedFrom(demos, resource3, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
gentrification_score.execute()
doc = gentrification_score.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
