import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class requestBigBelly(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = []
    writes = ['adsouza_mcsmocha.BigBelly']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        url = 'https://data.boston.gov/export/15e/7fa/15e7fa44-b9a8-42da-82e1-304e43460095.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BigBelly")
        repo.createCollection("BigBelly")
        repo['adsouza_mcsmocha.BigBelly'].insert_many(r)
        repo['adsouza_mcsmocha.BigBelly'].metadata({'complete':True})
        print(repo['adsouza_mcsmocha.BigBelly'].metadata())

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
        doc.add_namespace('anb', 'https://data.boston.gov/')

        this_script = doc.agent('alg:adsouza_mcsmocha#requestBigBelly', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('anb:15e7fa44-b9a8-42da-82e1-304e43460095', {'prov:label':'Big Belly Locations, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bb, this_script)
        doc.usage(get_bb, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval',
        	'ont:Query':'?type=Big+Belly+Locations&$description,location'
        	}
        	)

        big_belly = doc.entity('dat:adsouza_mcsmocha#BigBelly', {prov.model.PROV_LABEL:'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(big_belly, this_script)
        doc.wasGeneratedBy(big_belly, get_bb, endTime)
        doc.wasDerivedFrom(big_belly, resource, get_bb, get_bb, get_bb)

        repo.logout()

        return doc

 requestBigBelly.execute()
 doc = requestBigBelly.provenance()
 print(doc.get_provn())
 print(json.dumps(json.loads(doc.serialize()), indent=4))