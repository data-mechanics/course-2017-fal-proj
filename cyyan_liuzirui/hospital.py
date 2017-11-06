import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hospital(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = []
    writes = ['cyyan_liuzirui.hospital']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        url = 'https://data.boston.gov/export/622/208/6222085d-ee88-45c6-ae40-0c7464620d64.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hospital")
        repo.createCollection("hospital")
        # for i in r:
        #     new={}
        #     new[i]=r[i]
        #     repo['cyyan_liuzirui.hospital'].insert(new)
        repo['cyyan_liuzirui.hospital'].insert_many(r)
        #repo['cyyan_liuzirui.hospital'].metadata({'complete':True})
        #print(repo['cyyan_liuzirui.hospital'].metadata())

    
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
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/hospital-locations/resource/6222085d-ee88-45c6-ae40-0c7464620d64')

        this_script = doc.agent('alg:cyyan_liuzirui#hospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:cyyan_liuzirui#hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        get_hospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hospital, this_script)
        doc.usage(get_hospital, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        hospital = doc.entity('dat:cyyan_liuzirui#hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospital, this_script)
        doc.wasGeneratedBy(hospital, get_hospital, endTime)
        doc.wasDerivedFrom(hospital, resource, get_hospital, get_hospital, get_hospital)

        repo.logout()
                  
        return doc

# hospital.execute()
# doc = hospital.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
