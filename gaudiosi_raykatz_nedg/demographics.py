import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class demographics(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = []
    writes = ['gaudiosi_raykatz.demographics']

    @staticmethod
    def execute(trial = False):
        '''Retrieve racial demographics from US Census'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        url = "https://api.census.gov/data/2010/sf1?get=P016A001,P016B001,P016C001,P016D001,P016E001,P016H001,P0160001&for=zip+code+tabulation+area:*&in=state:25&key="
        
        url2 = "https://api.census.gov/data/2015/acs5?get=B11001_003E,B11001_001E,B23025_005E,B23025_003E,B23025_001E,B08101_025E,B08101_001E&for=zip+code+tabulation+area:02108,02109,02110,02111,02112,02113,02114,02115,02116,02117,02118,02119,02120,02121,02122,02123,02124,02125,02126,02127,02128,02129,02130,02131,02132,02133,02134,02135,02136,02137,02163,02196,02199,02201,02203,02204,02205,02206,02207,02210,02211,02212,02215,02216,02217,02222,02228,02241,02266,02283,02284,02293,02295,02297,02298&key="

        url += "432902274829a00579344cc87ee2c21713f4b476"
        url2 += "432902274829a00579344cc87ee2c21713f4b476"
        
        #with open('auth.json') as data_file:    
        #        data = json.load(data_file)
        #url += data["census"]
        #url2 += data["census"]
        #Returns the ordered by population numbers of [white, black, native american, asian, pacific islander, hispanic, total, state id (25), zipcode]
        response = urllib.request.urlopen(url).read().decode("utf-8")
        result = json.loads(response)

        response = urllib.request.urlopen(url2).read().decode("utf-8")
        result2 = json.loads(response)
        r = []
        for i in range(1,len(result)):
            if int(result[i][2]) == 0 or int(result[i][4]) ==0: 
               continue
            zipcode = result[i][8]
            d = {}
            d["white"] = int(result[i][0])
            d["black"] = int(result[i][1])
            d["native_american"] = int(result[i][2])
            d["asian"] = int(result[i][3])
            d["pacific_islander"] = int(result[i][4] )
            d["hispanic"] = int(result[i][5])
            d["total"] = int(result[i][6])
            for j in range(1, len(result2)):
                print("old zip =", zipcode, " new zip=", result2[j][7])
                if result2[j][7] == zipcode: 
                    if int(result2[j][6]) == 0:
                        continue
                    d["married_households"] = int(result2[j][0]) 
                    d["total_households"] = int(result2[j][1]) 
                    d["unemployed"] = int(result2[j][2]) 
                    d["labor_force"] = int(result2[j][3]) 
                    d["employment_count"] = int(result2[j][4]) 
                    d["public_transit"] = int(result2[j][5]) 
                    d["total_transit"] = int(result2[j][6]) 
            if "unemployed" not in d:
                continue

            d["zipcode"] = zipcode
            r.append(d)


        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("demographics")
        repo.createCollection("demographics")
        repo['gaudiosi_raykatz.demographics'].insert_many(r)
        repo['gaudiosi_raykatz.demographics'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.demographics'].metadata())
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
                  'ont:Query':'?type=Demographics&$select=white,black,native_american,asian,pacific_islander,hispanic,total,married_households,total_households,unemployed,labor_force,employment_count,public_transit,total_transit,zipcode'
                  }
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz#demographics', {prov.model.PROV_LABEL:'Demographics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc

demographics.execute()
doc = demographics.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
