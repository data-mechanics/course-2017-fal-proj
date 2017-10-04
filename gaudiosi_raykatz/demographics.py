import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class demographics(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = []
    writes = ['gaudiosi_katz.demographics']

    @staticmethod
    def execute(trial = False):
        '''Retrieve racial demographics from US Census'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        url = "https://api.census.gov/data/2010/sf1?get=P016A001,P016B001,P016C001,P016D001,P016E001,P016H001,P0160001&for=zip+code+tabulation+area:*&in=state:25&key="
        with open('auth.json') as data_file:    
                data = json.load(data_file)
        url += data["census"]
        
        #Returns the ordered by population numbers of [white, black, native american, asian, pacific islander, hispanic, total, state id (25), zipcode]
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        result = json.loads(response)
        r = []
        for i in range(1,len(result)):
            d = {}
            d["white"] = result[i][0]
            d["black"] = result[i][1]
            d["native_american"] = result[i][2]
            d["asian"] = result[i][3]
            d["pacific_islander"] = result[i][4] 
            d["hispanic"] = result[i][5]
            d["total"] = result[i][6]
            d["zipcode"] = result[i][8]
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
                  'ont:Query':'?type=Demographics&$select=white,black,native_american,asian,pacific_islander,hispanic,total,zipcode'
                  }
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz#demographics', {prov.model.PROV_LABEL:'Demographics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc
'''
demographics.execute()
doc = demographics.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
