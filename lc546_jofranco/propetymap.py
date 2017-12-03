import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class propetymap(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = []
    writes = ['lc546_jofranco.propety']

    #longitude = []
    #latitude = []
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        zipcode = []
        lalo = []
        street = []
        for i in r:
            zipcode += i['zipcode']
            lalo += [i['latitude'], i['longitude']]
            street += i['st_name']

        total = {'zipcode': zipcode, 'address': lalo, 'street': street}
    #    print(total)



        s = json.dumps(r, sort_keys= True, indent = 2)
    #    print(type(s))
        repo.dropCollection("propety")
        repo.createCollection("propety")
        repo["lc546_jofranco.propety"].insert_many([total])
        repo["lc546_jofranco.propety"].metadata({'complete':True})
        print(repo["lc546_jofranco.propety"].metadata())
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
        doc.add_namespace('bdp', 'https://secure.thepropety.com/data/')
        this_script = doc.agent('alg:lc546_jofranco#propety', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'propety', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_propetyinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'propety', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_propetyinfo, this_script)
        doc.usage(get_propetyinfo, resource, startTime)
        propetyinfo = doc.entity('dat:lc546_jofranco#propetyinfo', {prov.model.PROV_LABEL:'propety info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(propetyinfo, this_script)
        doc.wasGeneratedBy(propetyinfo, get_propetyinfo, endTime)
        doc.wasDerivedFrom(propetyinfo, resource, get_propetyinfo, get_propetyinfo, get_propetyinfo)
        return doc



propetymap.execute()
doc = propetymap.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
