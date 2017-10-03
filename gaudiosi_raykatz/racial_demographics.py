import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class racial_makeup(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = []
    writes = ['gaudiosi_katz.racial_makeup_2010']

    @staticmethod
    def execute(trial = False):
        '''Retrieve racial demographics from US Census'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        url = "https://api.census.gov/data/2010/sf1?get=P016A001,P016B001,P016C001,P016D001,P016E001,P016H001,P0160001&for=zip+code+tabulation+area:*&in=state:25&key="
        with open('../auth.json') as data_file:    
                data = json.load(data_file)
        url += data["census"]
        
        #Returns the ordered by population numbers of [white, black, native american, asian, pacific islander, hispanic, total, state id (25), zipcode]
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        print(s)
        repo.dropCollection("racial_makeup_2010")
        repo.createCollection("racial_makeup_2010")
        repo['gaudiosi_raykatz.racial_makeup_2010'].insert_many(r)
        repo['gaudiosi_raykatz.racial_makeup_2010'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.racial_makeup_2010'].metadata())
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
        get_race = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_race, this_script)
        
        doc.usage(get_race, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Property&$select=MAIL_ADDRESS,OWNER'
                  }
                  )
        
        race = doc.entity('dat:gaudiosi_raykatz#racial_makeup_2010', {prov.model.PROV_LABEL:'Racial Makeup 2010', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(race, this_script)
        doc.wasGeneratedBy(race, get_race, endTime)
        doc.wasDerivedFrom(race, resource, get_race, get_race, get_race)

        repo.logout()
                  
        return doc

racial_makeup.execute()
doc = racial_makeup.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
