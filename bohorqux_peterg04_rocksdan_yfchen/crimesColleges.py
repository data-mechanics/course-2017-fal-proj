import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimesColleges(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.colleges', 'bohorqux_peterg04_rocksdan_yfchen.crimes']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.crimes_colleges']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        colleges = repo['bohorqux_peterg04_rocksdan_yfchen.colleges']
        crimes = repo['bohorqux_peterg04_rocksdan_yfchen.crimes']

        college_reports = []
        crime_reports = []
        intersect = []

        #Create distinct list of streets corresponding to each database
        print("Parsing college...")
        for feature in colleges.find():
            whole_data = feature["features"]
            for reports in whole_data:
                street = reports["properties"]["Address"]
                if street not in college_reports:
                    college_reports.append(street.upper())

        print("Parsing Crimes...")
        for reports in crimes.find():
            if reports["STREET"] not in crime_reports:
                crime_reports.append(reports["STREET"])

        #Place identical street names in array
        for street in crime_reports:
            if street in college_reports:
                intersect.append(street)

        print("Shared Data:", str(len(intersect)))
        print(len(college_reports))
        print("******\n******\n******")
        print(len(crime_reports))
        print(intersect)
        #Create dictionary of streets, setting None as initial values

        #All information from both databases go under their respective street names.
        #Ex: BUSWELL ST: {
        #                  Crimes {
        #                            {All Data from the Crimes Database that occured on BUSWELL ST goes in here}
        #                         }
        #                  Properties {
        #                                {All Data from the Properties Database that occured on BUSWELL ST goes in here}
        #                             }
        #                 }
        
        p_c.append(intersect)
        repo.dropCollection("crimes_colleges")
        repo.createCollection("crimes_colleges")
        repo['bohorqux_peterg04_rocksdan_yfchen.crimes_colleges'].insert_many(p_c)
        
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

crimesColleges.execute()
#doc = crimesProperty.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
