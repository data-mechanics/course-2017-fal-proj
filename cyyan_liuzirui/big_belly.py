import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class big_belly(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = []
    writes = ['cyyan_liuzirui.big_belly']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        url = 'https://data.boston.gov/export/15e/7fa/15e7fa44-b9a8-42da-82e1-304e43460095.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("big_belly")
        repo.createCollection("big_belly")
        # for i in r:
        #     new={}
        #     new[i]=r[i]
        #     repo['cyyan_liuzirui.big_belly'].insert(new)
        repo['cyyan_liuzirui.big_belly'].insert_many(r)
        #repo['cyyan_liuzirui.big_belly'].metadata({'complete':True})
        #print(repo['cyyan_liuzirui.big_belly'].metadata())


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
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/big-belly-locations/resource/15e7fa44-b9a8-42da-82e1-304e43460095')

        this_script = doc.agent('alg:cyyan_liuzirui#big_belly', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:cyyan_liuzirui#big_belly', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_big_belly = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_big_belly, this_script)
        doc.usage(get_big_belly, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        big_belly = doc.entity('dat:cyyan_liuzirui#big_belly', {prov.model.PROV_LABEL:'big_belly', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(big_belly, this_script)
        doc.wasGeneratedBy(big_belly, get_big_belly, endTime)
        doc.wasDerivedFrom(big_belly, resource, get_big_belly, get_big_belly, get_big_belly)

        repo.logout()
                  
        return doc

# big_belly.execute()
# doc = big_belly.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
