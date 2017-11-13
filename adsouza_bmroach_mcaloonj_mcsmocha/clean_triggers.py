"""
Filename: clean_triggers.py

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

class clean_triggers(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters', 
             'adsouza_bmroach_mcaloonj_mcsmocha.hospitals', 
             'adsouza_bmroach_mcaloonj_mcsmocha.schools', 
             'adsouza_bmroach_mcaloonj_mcsmocha.open_space']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers']

    @staticmethod
    def execute(trial=False, logging=True):
        startTime = datetime.datetime.now()
        if logging:
            print("in clean_triggers.py")
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        repo.dropCollection('adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers')
        repo.createCollection('adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers')

        accident_clusters = repo['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters'].find()
        schools = repo['adsouza_bmroach_mcaloonj_mcsmocha.schools'].find()
        parks = repo['adsouza_bmroach_mcaloonj_mcsmocha.open_space'].find()
        hospitals = repo['adsouza_bmroach_mcaloonj_mcsmocha.hospitals'].find()

        #What gets added to db.clean_triggers should all be lists of tuples

        # Accident Clusters
        cluster_coords = [tuple(crd) for crd in accident_clusters[0]['accident_clusters']]

        # Schools
        sch_coord = [tuple(s['fields']['geo_shape']['coordinates'])[::-1] for s in schools]

        # Parks
        park_coord = []
        for p in parks:
            if p["geometry"]["type"] == "Polygon":
                first_coord = (tuple(p["geometry"]["coordinates"][0][0]))[::-1]
            else:
                first_coord = (tuple(p["geometry"]["coordinates"][0][0][0]))[::-1]
            park_coord.append(first_coord)

        # Hospitals
        hosp_coord = []
        for h in hospitals: #convert from strings to floats
            if h['YCOORD'] != 'NULL' or h['XCOORD'] != 'NULL':
                lat = float(h['YCOORD'][:2] + '.' + h['YCOORD'][2:])
                lng = -1*float(h['XCOORD'][:2] + '.' + h['XCOORD'][2:])
                hosp_coord.append((lat, lng))

        trigger_dict = {'accident_clusters': cluster_coords, 'schools': sch_coord, 'parks': park_coord, 'hospitals': hosp_coord}
        repo['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers'].insert_one(trigger_dict)

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

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_accidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

        get_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_accidents, this_script)

        doc.usage(get_accidents, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000'
                  }
                  )

        clean_triggers = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#clean_triggers', {prov.model.PROV_LABEL:'clean_triggers',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clean_triggers, this_script)
        doc.wasGeneratedBy(clean_triggers, get_accidents, endTime)
        doc.wasDerivedFrom(clean_triggers, resource, get_accidents, get_accidents, get_accidents)

        repo.logout()
        return doc

# clean_triggers.execute()
# doc = clean_triggers.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


##eof
