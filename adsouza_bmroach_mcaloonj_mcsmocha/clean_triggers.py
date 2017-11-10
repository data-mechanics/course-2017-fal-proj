import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class clean_triggers(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.accidents', 'adsouza_bmroach_mcaloonj_mcsmocha.hospitals', 'adsouza_bmroach_mcaloonj_mcsmocha.schools', 'adsouza_bmroach_mcaloonj_mcsmocha.open_space']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        repo.dropCollection('adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers')
        repo.createCollection('adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers')

        accidents = repo['adsouza_bmroach_mcaloonj_mcsmocha.accidents'].find()
        schools = repo['adsouza_bmroach_mcaloonj_mcsmocha.schools'].find()
        parks = repo['adsouza_bmroach_mcaloonj_mcsmocha.open_space'].find()
        hospitals = repo['adsouza_bmroach_mcaloonj_mcsmocha.hospitals'].find()

        acc = [a['Location'] for a in accidents]
        acc_coord = []
        # change each string tuple to tuple
        for a in acc:
            tup = tuple(map(float, a[1:-1].split(',')))
            if tup != (0,0):
                acc_coord.append(tup)
        sch_coord = [tuple(s['fields']['geo_shape']['coordinates'])[::-1] for s in schools]
        
        #park_coord = [tuple(p['geometry']['coordinates'][0][0])[::-1] for p in parks]
        park_coord = []
        for p in parks:
            if p["geometry"]["type"] == "Polygon":
                first_coord = (tuple(p["geometry"]["coordinates"][0][0]))[::-1]
            else:
                first_coord = (tuple(p["geometry"]["coordinates"][0][0][0]))[::-1]
            park_coord.append(first_coord)
        
        # the coordinates in the hospital dataset make the coordinates strings instead of floats, so cleaning it up
        # to make them usable
        hosp_coord = []
        for h in hospitals:
            if h['YCOORD'] != 'NULL' or h['XCOORD'] != 'NULL':
                lat = float(h['YCOORD'][:2] + '.' + h['YCOORD'][2:])
                long = float(h['XCOORD'][:2] + '.' + h['XCOORD'][2:])
                hosp_coord.append((lat, long))
        
        trigger_dict = {'accidents': acc_coord, 'schools': sch_coord, 'parks': park_coord, 'hospitals': hosp_coord}
        #print(trigger_dict)
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

clean_triggers.execute()
'''
doc = fetch_accidents.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
##eof