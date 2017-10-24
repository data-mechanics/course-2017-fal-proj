import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class police(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = []
    writes = ['cyyan_liuzirui.police']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("police")
        repo.createCollection("police")
        # for i in r:
        #     new={}
        #     new[i]=r[i]
        #     repo['cyyan_liuzirui.police'].insert(new)
        repo['cyyan_liuzirui.police'].insert_many(r)
        #repo['cyyan_liuzirui.police'].metadata({'complete':True})
        #print(repo['cyyan_liuzirui.police'].metadata())

    
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

        this_script = doc.agent('alg:cyyan_liuzirui#police', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:cyyan_liuzirui#police', {prov.model.PROV_LABEL:'police', prov.model.PROV_TYPE:'ont:DataSet'})
        get_police = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_police, this_script)
        doc.usage(get_police, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        police = doc.entity('dat:cyyan_liuzirui#police', {prov.model.PROV_LABEL:'police', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(police, this_script)
        doc.wasGeneratedBy(police, get_police, endTime)
        doc.wasDerivedFrom(police, resource, get_police, get_police, get_police)

        repo.logout()
                  
        return doc

# police.execute()
# doc = police.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
