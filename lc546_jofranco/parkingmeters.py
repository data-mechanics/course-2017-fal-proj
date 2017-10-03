import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrievedata(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.parking_meters']

    @staticmethod
    def execute(trial = False):
    	startTime = datetime.datetime.now()
    	# Set up the database connection.
    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate("lc546_jofranco", "lc546_jofranco")

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        reponse = urllib.request.urlopen(url).read().decode("urg-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys= True, indent = 2)
        repo.dropCollection("parking_meters")
        repo.createCollection("parking_meters")
        repo["lc546_jofranco.parking_meters"].insert_many(r)
        repo["lc546_jofranco.parking_meters"].metadata({'complete':True})
        print(repo["lc546_jofranco.parking_meters"].metadata())

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
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')
        this_script = doc.agent('alg:lc546_jofranco#retrievedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'Parking', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_parkinginfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Parking', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_parkinginfo, this_script)
        doc.usage(get_parkinginfo, resource, startTime)
        ParkingMeters = doc.entity('dat:lc546_jofranco#parking_meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ParkingMeters, this_script)
        doc.wasGeneratedBy(ParkingMeters, get_parkinginfo, endTime)
        doc.wasDerivedFrom(ParkingMeters, resource, get_parkinginfo, get_parkinginfo, get_parkinginfo)


        return doc



retrievedata.execute()
doc = retrievedata.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

