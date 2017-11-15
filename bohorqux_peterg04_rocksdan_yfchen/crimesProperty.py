import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimesProperty(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.properties', 'bohorqux_peterg04_rocksdan_yfchen.crimes']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.property_crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        limit = 1000 #cap records retrieval with this value. Setting limit to a value larger than 1000 may result in an error as a result of the file size being too big.
        startTime = datetime.datetime.now()
        print("Creating CrimesProperty...")
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        properties = repo['bohorqux_peterg04_rocksdan_yfchen.properties']
        crimes = repo['bohorqux_peterg04_rocksdan_yfchen.crimes']

        property_reports = []
        crime_reports = []
        intersect = []

        crimes_data = list(crimes.find())
        properties_data = list(properties.find())
        
        #Create distinct list of streets corresponding to each database
        for houses in properties_data:
            street = houses["ST_NAME"] + " " + houses["ST_NAME_SUF"]
            if street not in property_reports:
                property_reports.append(street)

        for reports in crimes_data:
            if reports["STREET"] not in crime_reports:
                crime_reports.append(reports["STREET"])

        #Place identical street names in array

        if(trial == True):
            limit = 100
            iterations = 0
            for street in property_reports:
                if street in crime_reports:
                    intersect.append(street)
                    iterations += 1
                if iterations == limit:
                    break
        else:
            iterations = 0
            for street in property_reports:
                if street in crime_reports:
                    intersect.append(street)
                    iterations += 1
                if iterations == limit:
                    break

        print("Shared Data:", str(len(intersect)))
        
        #Create dictionary of streets, setting None as initial values
        properties_crimes = dict().fromkeys(intersect)

        #All information from both databases go under their respective street names.
        #Ex: BUSWELL ST: {
        #                  Crimes {
        #                            {All Data from the Crimes Database that occured on BUSWELL ST goes in here}
        #                         }
        #                  Properties {
        #                                {All Data from the Properties Database that occured on BUSWELL ST goes in here}
        #                             }
        #                 }
        indice = 0
        
        for key in properties_crimes: #iterating through all street names
            
            #Insert Crimes data related to this street in this key
            if indice%10 == 0:
                print("{}/{} records parsed".format(indice, limit))
            
            properties_crimes[key] = {"Crimes": 0, "Properties": list()}
            for reports in crimes_data:
                if reports["STREET"] == key:
                    properties_crimes[key]["Crimes"] += 1
            
            #Insert Properties data related to this street in this key
            for reports in properties_data:
                street = reports["ST_NAME"] + " " + reports["ST_NAME_SUF"]
                if street == key:
                    properties_crimes[key]["Properties"].append({"ZIPCODE": reports["ZIPCODE"], "AVG_LAND": reports["AV_LAND"], "AVG_BLDG": reports["AV_BLDG"],
                                                                 "AVG_TOTAL": reports["AV_TOTAL"], "GROSS_TAX": reports["GROSS_TAX"], "Floors": reports["NUM_FLOORS"],
                                                                 "Year Built": reports["YR_BUILT"]})
            indice += 1        
            if indice == limit:
                break

        print("{}/{} records parsed: Parsing Complete".format(limit, limit))
        p_c = list()
        p_c.append(properties_crimes)
        
        repo.dropCollection("property_crimes")
        repo.createCollection("property_crimes")
        repo['bohorqux_peterg04_rocksdan_yfchen.property_crimes'].insert_many(p_c)
        
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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('prd', 'https://data.boston.gov/export/062/fc6/062fc6fa-b5ff-4270-86cf-202225e40858.json')
		doc.add_namespace('cd', 'https://data.boston.gov/export/12c/b38/12cb3883-56f5-47de-afa5-3b1cf61b257b.json')
		
        this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#crimesProperty', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        props = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#getProperties', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        crimes = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#getCrimes', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		merge_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		
		doc.wasAssociatedWith(merge_data, this_script)
        doc.usage(merge_data, props, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(merge_data, crimes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		
		merge = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#crimesProperty, {prov.model.PROV_LABEL: 'Merged Set', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(merge, this_script)
		doc.wasGeneratedBy(merge, merge_data, endTime)
		doc.wasDerivedFrom(merge, props, merge_data, merge_data, merge_data)
		doc.wasDerivedFrom(merge, crimes, merge_data, merge_data, merge_data)
		
        
        repo.logout()
                  
        return doc

#crimesProperty.execute(True)
#doc = crimesProperty.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
