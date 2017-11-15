import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class permitgeodata(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.permitgeodata']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        #print("this", response)
        r = json.loads(response)
        foodlocale = list()
        for locations in r:
            coor = {
            'location': locations['location']
            }
            foodlocale.append(coor)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropCollection("permitgeodata")
        repo.createCollection("permitgeodata")
    #    repo["permitgeo"].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        repo["lc546_jofranco.permitgeodata"].insert_many(foodlocale)
        repo["lc546_jofranco.permitgeodata"].metadata({'complete':True})
        print(repo["lc546_jofranco.permitgeodata"].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        this_script = doc.agent('alg:lc546_jofranco#retrievedatageo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'permitgeo', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_permitinfogeo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'permitgeo', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_permitinfogeo, this_script)
        doc.usage(get_permitinfogeo, resource, startTime)
        PermitinfoGeo = doc.entity('dat:lc546_jofranco#permitinfogeo', {prov.model.PROV_LABEL:'restaurant near hubway', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(PermitinfoGeo, this_script)
        doc.wasGeneratedBy(PermitinfoGeo, get_permitinfogeo, endTime)
        doc.wasDerivedFrom(PermitinfoGeo, resource, get_permitinfogeo, get_permitinfogeo, get_permitinfogeo)
        return doc



permitgeodata.execute()
doc = permitgeodata.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
