import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getCollege(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = []
    writes = ['bohorqux_peterg04_rocksdan_yfchen.college']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        url = 'http://datamechanics.io/data/bohorqux_rocksdan/college_cleaned.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace(']', "")
        response += ']'
        r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("college")
        repo.createCollection("college")
        repo['bohorqux_peterg04_rocksdan_yfchen.college'].insert_many(r)
        repo['bohorqux_peterg04_rocksdan_yfchen.college'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.college'].metadata())

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

        this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#getCollege', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_college = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_college, this_script)
        doc.usage(get_college, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?FIELD5=null&$select=FIELD5,FIELD2'
                  }
                  )
        college = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#college', {prov.model.PROV_LABEL:'Colleges', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(college, this_script)
        doc.wasGeneratedBy(college, get_college, endTime)
        doc.wasDerivedFrom(college, resource, get_college, get_college, get_college)

        repo.logout()
                  
        return doc
# 
# getCollege.execute()
# doc = getCollege.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
