import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class fetch_speed_complaints(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = []
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.speed_complaints']

        @staticmethod
        def execute(trial=False):
            startTime = datetime.datetime.now()
            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

            url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=80322d69-c46f-4b93-9c38-88e78ae59a34&q=people%20speed&limit=5000'

            response = requests.get(url)
            r = response.json()
            s = json.dumps(r, sort_keys=True, indent=2)
            #print (s)

            repo.dropCollection("speed_complaints")
            repo.createCollection("speed_complaints")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.speed_complaints'].insert_many(r["result"]["records"])
            repo['adsouza_bmroach_mcaloonj_mcsmocha.speed_complaints'].metadata({'complete':True})
            #print(repo['adsouza_bmroach_mcaloonj_mcsmocha.speed_complaints'].metadata())

            repo.logout()

            endTime = datetime.datetime.now()

            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
            doc.add_namespace('dbg','https://data.boston.gov/api/3/action/datastore_search')

            this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_speed_complaints', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
            resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Vision Zero Entry', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

            get_speed_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_speed_complaints, this_script)

            doc.usage(get_speed_complaints, resource, startTime, None,
                      {prov.model.PROV_TYPE:'ont:Retrieval',
                      'ont:Query':'?resource_id=80322d69-c46f-4b93-9c38-88e78ae59a34&q=people%20speed&limit=5000'
                      }
                      )

            speed_complaints = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#speed_complaints', {prov.model.PROV_LABEL:'Speed Complaints submitted to Vision Zero',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(speed_complaints, this_script)
            doc.wasGeneratedBy(speed_complaints, get_speed_complaints, endTime)
            doc.wasDerivedFrom(speed_complaints, resource, get_speed_complaints, get_speed_complaints, get_speed_complaints)

            repo.logout()
            return doc
'''
fetch_speed_complaints.execute()
doc = fetch_speed_complaints.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
##eof
