import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieverealtime(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.realtime_MBTA']

    @staticmethod
    def execute(trial = False):
    	startTime = datetime.datetime.now()
    	# Set up the database connection.
    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate("lc546_jofranco", "lc546_jofranco")		
    	url = 'http://Developer.mbta.com/lib/rthr/red.json'
    	response = urllib.request.urlopen(url).read().decode("utf-8")
    	r = json.loads(response)
    	s = json.dumps(r, sort_keys=True, indent=2)
    	repo.dropCollection("realtime_MBTA")
    	repo.createCollection("realtime_MBTA")
    	repo['lc546_jofranco.realtime_MBTA'].insert_many(r)
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
    	doc.add_namespace('bdp', 'http://old.mbta.com/rider_tools/developers/default.asp?id=21898')

    	this_script = doc.agent('alg:lc546_jofranco#MBTAredlinedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    	resource = doc.entity('bdp:xgbq-327x', {'prov:label':'RealTimeRedLine', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	get_MBTAinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'RealTimeRedLine', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAssociatedWith(get_MBTAinfo, this_script)
    	doc.usage(get_MBTAinfo, resource, startTime)
    	MBTAReport = doc.entity('dat:lc546_jofranco#RealTimeRedLine', {prov.model.PROV_LABEL:'Real Time', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(MBTAReport, this_script)
    	doc.wasGeneratedBy(MBTAReport, get_MBTAinfo, endTime)
    	doc.wasDerivedFrom(MBTAReport, resource, get_MBTAinfo, get_MBTAinfo, get_MBTAinfo)
    	return doc



retrieverealtime.execute()
doc = retrieverealtime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))