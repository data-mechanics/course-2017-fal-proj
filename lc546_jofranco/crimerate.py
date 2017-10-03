import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrievedata(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.crime_rate']

    @staticmethod
    def execute(trial = False):
    	startTime = datetime.datetime.now()
    	# Set up the database connection.
    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate("lc546_jofranco", "lc546_jofranco")

        url = 'https://data.cityofboston.gov/resource/crime.json'
        reponse = urllib.request.urlopen(url).read().decode("urg-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys= True, indent = 2)
        repo.dropCollection("crimerate")
        repo.createCollection("crimerate")
        repo["lc546_jofranco.crimerate"].insert_many(r)
        repo["lc546_jofranco.crimerate"].metadata({'complete':True})
        print(repo["lc546_jofranco.crimerate"].metadata())

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
        this_script = doc.agent('alg:lc546_jofranco#retrievedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'crimerate', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimeinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'crimerate', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_crimeinfo, this_script)
        doc.usage(get_crimeinfo, resource, startTime)
        Crimeinfo = doc.entity('dat:lc546_jofranco#crimerate', {prov.model.PROV_LABEL:'Crime Rate', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Crimeinfo, this_script)
        doc.wasGeneratedBy(Crimeinfo, get_crimeinfo, endTime)
        doc.wasDerivedFrom(Crimeinfo, resource, get_crimeinfo, get_crimeinfo, get_crimeinfo)


        return doc



retrievedata.execute()
doc = retrievedata.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

