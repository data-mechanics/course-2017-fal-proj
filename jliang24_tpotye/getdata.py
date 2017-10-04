import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getdata(dml.Algorithm):
    contributor = 'jliang24_tpotye'
    reads = []
    writes = ['jliang24_tpotye.properties', 'jliang24_tpotye.police', 'jliang24_tpotye.doc_311', 'jliang24_tpotye.potholes', 'jliang24_tpotye.hospital']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jliang24_tpotye', 'jliang24_tpotye')

        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("properties")
        repo.createCollection("properties")
        repo['jliang24_tpotye.properties'].insert_many(r)

        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        a = json.loads(response)
        b = json.dumps(a, sort_keys=True, indent=2)
        repo.dropCollection("police")
        repo.createCollection("police")
        repo['jliang24_tpotye.police'].insert_many(a)

        url = 'https://data.cityofboston.gov/resource/wc8w-nujj.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        c = json.loads(response)
        d = json.dumps(c, sort_keys=True, indent=2)
        repo.dropCollection("doc_311")
        repo.createCollection("doc_311")
        repo['jliang24_tpotye.doc_311'].insert_many(c)

        url = 'https://data.nlc.org/resource/5udy-aqqy.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        e = json.loads(response)
        f = json.dumps(e, sort_keys=True, indent=2)
        repo.dropCollection("potholes")
        repo.createCollection("potholes")
        repo['jliang24_tpotye.potholes'].insert_many(e)

        url = 'https://data.boston.gov/export/622/208/6222085d-ee88-45c6-ae40-0c7464620d64.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        g = json.loads(response)
        h = json.dumps(g, sort_keys=True, indent=2)
        repo.dropCollection("hospital")
        repo.createCollection("hospital")
        repo['jliang24_tpotye.hospital'].insert_many(g)

        #print('DONE!')

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
        repo.authenticate('jliang24_tpotye', 'jliang24_tpotye')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdp1', 'https://data.nlc.org/resource/')
        doc.add_namespace('bdp2', 'https://data.boston.gov/export/622/208/')
        

        this_script = doc.agent('alg:jliang24_tpotye#getdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Assessments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_properties = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_properties, this_script)
        doc.usage(get_properties, resource_properties, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})


        resource_police = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_police = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_police, this_script)
        doc.usage(get_police, resource_police, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
 

        resource_311 = doc.entity('bdp:wc8w-nujj', {'prov:label':'311 Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_311, this_script)
        doc.usage(get_311, resource_311, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        
        resource_potholes = doc.entity('bdp1:5udy-aqqy', {'prov:label':'Potholes Repaired', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_potholes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_potholes, this_script)
        doc.usage(get_potholes, resource_potholes, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        resource_hospital = doc.entity('bdp2:6222085d-ee88-45c6-ae40-0c7464620d64', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hospital, this_script)
        doc.usage(get_hospital, resource_hospital, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})


        properties = doc.entity('dat:jliang24_tpotye#properties', {prov.model.PROV_LABEL:'Property Assessments', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties, this_script)
        doc.wasGeneratedBy(properties, get_properties, endTime)
        doc.wasDerivedFrom(properties, resource_properties, get_properties, get_properties, get_properties)

        police = doc.entity('dat:jliang24_tpotye#police', {prov.model.PROV_LABEL:'Police Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(police, this_script)
        doc.wasGeneratedBy(police, get_police, endTime)
        doc.wasDerivedFrom(police, resource_police, get_police, get_police, get_police)

        doc_311 = doc.entity('dat:jliang24_tpotye#doc_311', {prov.model.PROV_LABEL:'311 Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(doc_311, this_script)
        doc.wasGeneratedBy(doc_311, get_311, endTime)
        doc.wasDerivedFrom(doc_311, resource_311, get_311, get_311, get_311)

        potholes = doc.entity('dat:jliang24_tpotye#potholes', {prov.model.PROV_LABEL:'Potholes Repaired', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(potholes, this_script)
        doc.wasGeneratedBy(potholes, get_potholes, endTime)
        doc.wasDerivedFrom(potholes, resource_potholes, get_potholes, get_potholes, get_potholes)

        hospital = doc.entity('dat:jliang24_tpotye#hospital', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospital, this_script)
        doc.wasGeneratedBy(hospital, get_hospital, endTime)
        doc.wasDerivedFrom(hospital, resource_hospital, get_hospital, get_hospital, get_hospital)

        repo.logout()
                  
        return doc

##getdata.execute()
##doc = getdata.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))
