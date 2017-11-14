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
        startTime = datetime.datetime.now()
        print("Creating CrimesProperty...")
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        # 244 Streets in common. p has less streets than c
        properties = repo['bohorqux_peterg04_rocksdan_yfchen.properties']
        crimes = repo['bohorqux_peterg04_rocksdan_yfchen.crimes']

        property_reports = []
        crime_reports = []
        intersect = []

        #Create distinct list of streets corresponding to each database
        print("Parsing houses...")
        for houses in properties.find():
            street = houses["ST_NAME"] + " " + houses["ST_NAME_SUF"]
            if street not in property_reports:
                property_reports.append(street)

        print("Parsing Crimes...")
        for reports in crimes.find():
            if reports["STREET"] not in crime_reports:
                crime_reports.append(reports["STREET"])

        #Place identical street names in array
        if(trial == True):
            iterations = 0
            for street in property_reports:
                if street in crime_reports:
                    intersect.append(street)
                    iterations += 1
                if iterations == 10:
                    break
        else:
            for street in property_reports:
                if street in crime_reports:
                    intersect.append(street)

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
            print(key)
            
            properties_crimes[key] = {"Crimes": 0, "Properties": list()}
            for reports in crimes.find():
                if reports["STREET"] == key:
                   # properties_crimes[key]["Crimes"].append({"Offense Code": reports["OFFENSE_CODE_GROUP"], "Description": reports["OFFENSE_DESCRIPTION"], "Shooting": reports["SHOOTING"],                    "Location": reports["Location"]})
                    properties_crimes[key]["Crimes"] += 1
            print('*****' + key + " CRIMES COMPLETE")
            
            #Insert Properties data related to this street in this key
            for reports in properties.find():
                street = reports["ST_NAME"] + " " + reports["ST_NAME_SUF"]
                if street == key:
                    properties_crimes[key]["Properties"].append({"ZIPCODE": reports["ZIPCODE"], "AVG_LAND": reports["AV_LAND"], "AVG_BLDG": reports["AV_BLDG"],
                                                                 "AVG_TOTAL": reports["AV_TOTAL"], "GROSS_TAX": reports["GROSS_TAX"], "Floors": reports["NUM_FLOORS"],
                                                                 "Year Built": reports["YR_BUILT"]})
            indice += 1        
            print('-----' + key + " PROPERTIES COMPLETE" + "\t" + str(indice))
        
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#crimesProperty', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_properties = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_properties, this_script)
        doc.usage(get_properties, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?Street=TREMONT ST&$select=Street'
                  }
                  )
        properties = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#property_crimes', {prov.model.PROV_LABEL:'Property Values', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties, this_script)
        doc.wasGeneratedBy(properties, get_properties, endTime)
        doc.wasDerivedFrom(properties, resource, get_properties, get_properties, get_properties)

        repo.logout()
                  
        return doc

# crimesProperty.execute()
#doc = crimesProperty.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
