"""
Filename: fetch_hospitals.py

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

class fetch_hospitals(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = []
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.hospitals']

        @staticmethod
        def execute(trial=False, logging=True):
            startTime = datetime.datetime.now()

            if logging:
                print("in fetch_hospitals.py")

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

            url = 'https://data.boston.gov/export/622/208/6222085d-ee88-45c6-ae40-0c7464620d64.json'

            response = requests.get(url)
            r = response.json()
            s = json.dumps(r, sort_keys=True, indent=2)

            repo.dropCollection("hospitals")
            repo.createCollection("hospitals")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.hospitals'].insert_many(r)
            repo['adsouza_bmroach_mcaloonj_mcsmocha.hospitals'].metadata({'complete':True})

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
            doc.add_namespace('dbg','https://data.boston.gov/export/622/208/')

            #Agent
            this_script = doc.agent('alg:fetch_hospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

            #Resources
            resource = doc.entity('dbg:6222085d-ee88-45c6-ae40-0c7464620d64', {'prov:label': 'Hospitals', prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'json' })

            #Activities
            this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

            #Usage
            doc.wasAssociatedWith(this_run, this_script)
            doc.used(this_run, resource, startTime)

            hospitals = doc.entity('dat:hospitals', {prov.model.PROV_LABEL:'hospitals',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(hospitals, this_script)
            doc.wasGeneratedBy(hospitals, this_run, endTime)
            doc.wasDerivedFrom(hospitals, resource, this_run, this_run, this_run)


            repo.logout()
            return doc

# fetch_hospitals.execute()
# doc = fetch_hospitals.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
