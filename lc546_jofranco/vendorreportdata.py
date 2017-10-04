import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class vendorreportdata(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.vendor_report']

    @staticmethod
    def execute(trial = False):
    	startTime = datetime.datetime.now()
    	# Set up the database connection.
    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate("lc546_jofranco", "lc546_jofranco")
    	url = "https://data.cityofboston.gov/resource/xgbq-327x.json"
    	response = urllib.request.urlopen(url).read().decode("utf-8")
    	r = json.loads(response)
    	s = json.dumps(r, sort_keys=True, indent=2)
    	repo.dropCollection("vendor_report")
    	repo.createCollection("vendor_report")
    	repo["lc546_jofranco.vendor_report"].insert_many(r)
    	repo["lc546_jofranco.vendor_report"].metadata({'complete':True})
       # print(repo["lc546_jofranco.vendor_report"].metadata())
#print(repo["lc546_jofranco.vendor_report"].metadata())

        
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
    	this_script = doc.agent('alg:lc546_jofranco#vendorreportdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    	resource = doc.entity('bdp:xgbq-327x', {'prov:label':'Vendor', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    	get_vendorinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Vendors', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAssociatedWith(get_vendorinfo, this_script)
    	doc.usage(get_vendorinfo, resource, startTime)
    	VendorReport = doc.entity('dat:lc546_jofranco#VendorReport', {prov.model.PROV_LABEL:'Vendor Report', prov.model.PROV_TYPE:'ont:DataSet'})
    	doc.wasAttributedTo(VendorReport, this_script)
    	doc.wasGeneratedBy(VendorReport, get_vendorinfo, endTime)
    	doc.wasDerivedFrom(VendorReport, resource, get_vendorinfo, get_vendorinfo, get_vendorinfo)


    	return doc




vendorreportdata.execute()
doc = vendorreportdata.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
#doc = example.provenance()

#print(json.dumps(json.loads(doc.serialize()), indent=4))
