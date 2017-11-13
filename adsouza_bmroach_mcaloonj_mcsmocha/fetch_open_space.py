"""
Filename: fetch_open_space.py

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

class fetch_open_space(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = []
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.open_space']

    @staticmethod
    def execute(trial = False, logging=True):
        startTime = datetime.datetime.now()

        if logging:
                print("in fetch_open_space.py")

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson" #ArcGIS Open Data
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)

        repo.dropCollection("adsouza_bmroach_mcaloonj_mcsmocha.open_space")
        repo.createCollection("adsouza_bmroach_mcaloonj_mcsmocha.open_space")
        repo["adsouza_bmroach_mcaloonj_mcsmocha.open_space"].insert_many(r["features"])

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

        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_open_space', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7.geojson', {'prov:label':'Open Spaces', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_open_space = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_open_space, this_script)

        doc.usage(get_open_space, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval'})

        open_space = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#open_space', {prov.model.PROV_LABEL:'Open Spaces', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_space, this_script)
        doc.wasGeneratedBy(open_space, get_open_space, endTime)
        doc.wasDerivedFrom(open_space, resource, get_open_space, get_open_space, get_open_space)

        repo.logout()

        return doc

# fetch_open_space.execute()
# doc = fetch_open_space.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
