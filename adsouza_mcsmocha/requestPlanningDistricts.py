import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class requestPlanningDistricts(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = []
    writes = ['adsouza_mcsmocha.PlanningDistricts']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        url = 'https://data.opendatasoft.com/api/records/1.0/search/?dataset=planning-districts%40boston'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("PlanningDistricts")
        repo.createCollection("PlanningDistricts")
        repo['adsouza_mcsmocha.PlanningDistricts'].insert_many(r)
        repo['adsouza_mcsmocha.PlanningDistricts'].metadata({'complete':True})
        print(repo['adsouza_mcsmocha.PlanningDistricts'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')
       	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        # Additional resource
        doc.add_namespace('ods', 'https://data.opendatasoft.com/')

        this_script = doc.agent('alg:adsouza_mcsmocha#requestPlanningDistricts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('anb:planning-districts@boston', {'prov:label':'Planning Districts, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_plandist = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_plandist, this_script)
        doc.usage(get_plandist, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval',
        	'ont:Query':'?type=Planning+Districts&$objectid,geo_point_2d,shapearea,pd,id'
        	}
        	)

        plandist = doc.entity('dat:adsouza_mcsmocha#PlanningDistricts', {prov.model.PROV_LABEL:'Planning Districts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(plandist, this_script)
        doc.wasGeneratedBy(plandist, get_plandist, endTime)
        doc.wasDerivedFrom(plandist, resource, get_plandist, get_plandist, get_plandist)

        repo.logout()

        return doc

requestPlanningDistricts.execute()
doc = requestPlanningDistricts.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))