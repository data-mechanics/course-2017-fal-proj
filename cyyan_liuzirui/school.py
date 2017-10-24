import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class school(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = []
    writes = ['cyyan_liuzirui.school']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        url = 'https://data.cityofboston.gov/resource/pzcy-jpz4.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("school")
        repo.createCollection("school")
        # for i in r:
        #     new={}
        #     new[i]=r[i]
        #     repo['cyyan_liuzirui.school'].insert(new)
        repo['cyyan_liuzirui.school'].insert_many(r)
        #repo['cyyan_liuzirui.school'].metadata({'complete':True})
        #print(repo['cyyan_liuzirui.school'].metadata())

    
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
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/')

        this_script = doc.agent('alg:cyyan_liuzirui#school', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school, this_script)
        doc.usage(get_school, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+school&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        school = doc.entity('dat:cyyan_liuzirui#school', {prov.model.PROV_LABEL:'Animals school', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school, this_script)
        doc.wasGeneratedBy(school, get_school, endTime)
        doc.wasDerivedFrom(school, resource, get_school, get_school, get_school)

        repo.logout()
                  
        return doc

# school.execute()
# doc = school.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
