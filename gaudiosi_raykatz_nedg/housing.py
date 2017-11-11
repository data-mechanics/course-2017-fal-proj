import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class housing(dml.Algorithm):
    contributor = 'gaudiosi_raykatz_nedg'
    reads = []
    writes = ['gaudiosi_raykatz_nedg.housing']

    @staticmethod
    def execute(trial = False):
        '''Retrieve housing data from US Census'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
        url = "https://api.census.gov/data/2015/acs5?get=B25002_002E,B25002_003E,B25002_001E,B25034_011E,B25034_001E,B25003_003E,B25003_001E&for=zip+code+tabulation+area:02021,02108,02109,02110,02111,02112,02113,02114,02115,02116,02117,02118,02119,02120,02121,02122,02123,02124,02125,02126,02127,02128,02129,02130,02131,02132,02133,02134,02135,02136,02137,02163,02196,02199,02201,02203,02204,02205,02206,02207,02210,02211,02212,02215,02216,02217,02222,02228,02241,02266,02283,02284,02293,02295,02297,02298,02459,02151,02186,02026,02152,02467&key="
        with open('auth.json') as data_file:    
                data = json.load(data_file)
        url += data["census"]
        
        #Returns the ordered by population numbers of [occupied housing, vacant housing,housing,total housing,before 1939,total struct age]
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        result = json.loads(response)
        r = []
        for i in range(1,len(result)):
            if int(result[i][2]) == 0 or int(result[i][4]) ==0:
                continue
            d = {}
            d["occupied_housing"] = int(result[i][0])
            d["vacant_housing"] = int(result[i][1])
            d["total_housing"] = int(result[i][2])
            d["structures_built_before_1939"] = int(result[i][3])
            d["total_structures_built"] = int(result[i][4])
            d["renter_occupied"] = int(result[i][5])
            d["total_occupied"] = int(result[i][6])
            d["zipcode"] = result[i][7]
            r.append(d)
        
        s = json.dumps(r, sort_keys=True, indent=2)
        
        
        repo.dropCollection("housing")
        repo.createCollection("housing")
        repo['gaudiosi_raykatz_nedg.housing'].insert_many(r)
        repo['gaudiosi_raykatz_nedg.housing'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz_nedg.housing'].metadata())
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
        get_housing = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_housing, this_script)
        
        doc.usage(get_housing, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Housing&$select=occupied_housing,vacant_housing,total_housing,structures_build_before_1939,total_structures_built,zipcode'
                  }
                  )
        
        housing = doc.entity('dat:gaudiosi_raykatz_nedg#housing', {prov.model.PROV_LABEL:'Housing', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(housing, this_script)
        doc.wasGeneratedBy(housing, get_housing, endTime)
        doc.wasDerivedFrom(housing, resource, get_housing, get_housing, get_housing)

        repo.logout()
                  
        return doc

'''
housing.execute()
doc = housing.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
