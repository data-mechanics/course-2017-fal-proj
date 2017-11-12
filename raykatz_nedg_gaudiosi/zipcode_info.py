import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class zipcode_info(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = ["raykatz_nedg_gaudiosi.zipcode_map", "raykatz_nedg_gaudiosi.demographic_percentages", "raykatz_nedg_gaudiosi.housing_percentages", "raykatz_nedg_gaudiosi.income_percentages", "raykatz_nedg_gaudiosi.mbta_stops"]
    writes = ['raykatz_nedg_gaudiosi.zipcode_info']

    @staticmethod
    def execute(trial = False):
        '''Merge zipcode info'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
       
        r = []
        
        zipcode_data = list(repo.raykatz_nedg_gaudiosi.zipcode_map.find({}))[0]

        zipcode_list = []
        for feature in zipcode_data["features"]:
            zipcode_list.append(feature['properties']['ZIP5'])
        
        zipcode_list = list(set(zipcode_list))
        
        for zipcode in zipcode_list:
            z = {}
            z["zipcode"] = zipcode
            demographics = list(repo.raykatz_nedg_gaudiosi.demographic_percentages.find({"zipcode": zipcode}))
            income =  list(repo.raykatz_nedg_gaudiosi.income_percentages.find({"zipcode": zipcode}))
            if len(demographics) == 0 or len(income) == 0:
                continue
            else:
                income = income[0]
                demographics = demographics[0]
            
            
            housing = list(repo.raykatz_nedg_gaudiosi.housing_percentages.find({"zipcode": zipcode}))[0]
            
            
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
            z["percent_renting"] = housing["percent_renting"]

            z["subway_stops"] = repo.raykatz_nedg_gaudiosi.mbta_stops.count({"zipcode": zipcode, "mode_name": "Subway"})
            z["commuter_stops"] = repo.raykatz_nedg_gaudiosi.mbta_stops.count({"zipcode": zipcode, "mode_name": "Commuter Rail"})
            z["bus_stops"] = repo.raykatz_nedg_gaudiosi.mbta_stops.count({"zipcode": zipcode, "mode_name": "Bus"})
            
            
            r.append(z)
            
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("zipcode_info")
        repo.createCollection("zipcode_info")
        repo['raykatz_nedg_gaudiosi.zipcode_info'].insert_many(r)
        repo['raykatz_nedg_gaudiosi.zipcode_info'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.zipcode_info'].metadata())
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

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource =  doc.entity('dat:raykatz_nedg_gaudiosi#demographics_percentages', {'prov:label':'Demographic Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:raykatz_nedg_gaudiosi#housing_percentages', {'prov:label':'Housing Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
        resource3 = doc.entity('dat:raykatz_nedg_gaudiosi#income_percentages', {'prov:label':'Income Percentages', prov.model.PROV_TYPE:'ont:DataSet'})
        resource4 = doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_map', {'prov:label':'Zipcode Map', prov.model.PROV_TYPE:'ont:DataSet'}) 
        resource5 = doc.entity('dat:raykatz_nedg_gaudiosi#mbta_stops', {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        

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

        doc.usage(get_demos, resource4, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )

        doc.usage(get_demos, resource5, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                  )       
        
        demos = doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_info', {prov.model.PROV_LABEL:'Zipcode Info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)
        doc.wasDerivedFrom(demos, resource2, get_demos, get_demos, get_demos)
        doc.wasDerivedFrom(demos, resource3, get_demos, get_demos, get_demos)
        doc.wasDerivedFrom(demos, resource4, get_demos, get_demos, get_demos)
        doc.wasDerivedFrom(demos, resource5, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
zipcode_info.execute()
doc = zipcode_info.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
