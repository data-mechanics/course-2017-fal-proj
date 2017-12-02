"""
Filename: fetch_accidents.py

Last edited by: BMR 11/11/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Original skeleton files provided by Andrei Lapets (lapets@bu.edu)

Development Notes:


"""
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class fetch_accidents(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = []
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.accidents']

        @staticmethod
        def execute(trial=False, logging=True):
            startTime = datetime.datetime.now()

            if logging:
                print("in fetch_accidents.py")

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

            url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000'

            response = requests.get(url)
            r = response.json()
            s = json.dumps(r, sort_keys=True, indent=2)

            repo.dropCollection("accidents")
            repo.createCollection("accidents")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.accidents'].insert_many(r["result"]["records"])
            repo['adsouza_bmroach_mcaloonj_mcsmocha.accidents'].metadata({'complete':True})

            repo.logout()

            endTime = datetime.datetime.now()

            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/adsouza_bmroach_mcaloonj_mcsmocha/')
            doc.add_namespace('dat', 'http://datamechanics.io/data/adsouza_bmroach_mcaloonj_mcsmocha/')
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log#')
            doc.add_namespace('dbg','https://data.boston.gov/api/3/action/datastore_search')

            #Agent
            this_script = doc.agent('alg:fetch_accidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

            #Resources
            resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource'})

            #Activities
            this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000'})

            #Usage
            doc.wasAssociatedWith(this_run, this_script)
            doc.used(this_run, resource, startTime)

            #New dataset
            accidents = doc.entity('dat:accidents', {prov.model.PROV_LABEL:'Accidents',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(accidents, this_script)
            doc.wasGeneratedBy(accidents, this_run, endTime)
            doc.wasDerivedFrom(accidents, resource, this_run, this_run, this_run)

            repo.logout()
            return doc

# fetch_accidents.execute()
# doc = fetch_accidents.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
