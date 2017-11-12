"""
Filename: fetch_schools.py

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

class fetch_schools(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = []
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.schools']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        if trial:
                print("in fetch_schools.py")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        url = 'https://boston.opendatasoft.com/api/records/1.0/search/?dataset=public-schools&rows=-1'
        response = requests.get(url)
        r = response.json()
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("adsouza_bmroach_mcaloonj_mcsmocha.schools")
        repo.createCollection("adsouza_bmroach_mcaloonj_mcsmocha.schools")

        repo['adsouza_bmroach_mcaloonj_mcsmocha.schools'].insert_many(r["records"])
        repo['adsouza_bmroach_mcaloonj_mcsmocha.schools'].metadata({'complete':True})

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
        doc.add_namespace('bods', 'http://boston.opendatasoft.com/api/records/1.0/search/')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_schools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        resource = doc.entity('bods:'+str(uuid.uuid4()), {'prov:label': 'Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?dataset=public-schools&rows=-1'
                  }
                  )

        schools = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#schools', {prov.model.PROV_LABEL:'Public Schools',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource, get_schools, get_schools, get_schools)

        repo.logout()
        return doc


# fetch_schools.execute()
# doc = fetch_schools.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
