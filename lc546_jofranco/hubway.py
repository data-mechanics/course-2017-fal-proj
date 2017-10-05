
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hubway(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.hubwaybike']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
    	# Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        url = 'https://secure.thehubway.com/data/stations.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        stations = r['stations']
        #print(type(stations))
       # print(stations)
       # print(type(r))
        s = json.dumps(r, sort_keys= True, indent = 2)
        print(type(s))
        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        repo["lc546_jofranco.hubway"].insert_many(stations)
        repo["lc546_jofranco.hubway"].metadata({'complete':True})
        print(repo["lc546_jofranco.hubway"].metadata())
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
        doc.add_namespace('bdp', 'https://secure.thehubway.com/data/')
        this_script = doc.agent('alg:lc546_jofranco#hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'Hubway', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bikeinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'hubway', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_bikeinfo, this_script)
        doc.usage(get_bikeinfo, resource, startTime)
        Bikeinfo = doc.entity('dat:lc546_jofranco#Bikeinfo', {prov.model.PROV_LABEL:'Hubway Bike info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Bikeinfo, this_script)
        doc.wasGeneratedBy(Bikeinfo, get_bikeinfo, endTime)
        doc.wasDerivedFrom(Bikeinfo, resource, get_bikeinfo, get_bikeinfo, get_bikeinfo)
        return doc



hubway.execute()
doc = hubway.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

