import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class garden(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = []
    writes = ['cyyan_liuzirui.garden']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        url = 'https://data.cityofboston.gov/resource/rdqf-ter7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("garden")
        repo.createCollection("garden")
        # for i in r:
        #     new={}
        #     new[i]=r[i]
        #     repo['cyyan_liuzirui.garden'].insert(new)
        repo['cyyan_liuzirui.garden'].insert_many(r)
        #repo['cyyan_liuzirui.garden'].metadata({'complete':True})
        #print(repo['cyyan_liuzirui.garden'].metadata())

    
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

        this_script = doc.agent('alg:cyyan_liuzirui#garden', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:cyyan_liuzirui#garden', {prov.model.PROV_LABEL:'garden', prov.model.PROV_TYPE:'ont:DataSet'})
        get_garden = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_garden, this_script)
        doc.usage(get_garden, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        garden = doc.entity('dat:cyyan_liuzirui#garden', {prov.model.PROV_LABEL:'garden', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(garden, this_script)
        doc.wasGeneratedBy(garden, get_garden, endTime)
        doc.wasDerivedFrom(garden, resource, get_garden, get_garden, get_garden)

        repo.logout()
                  
        return doc

# garden.execute()
# doc = garden.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
