import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class requestCrimeData(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = []
    writes = ['adsouza_mcsmocha.CrimeData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        url = 'https://data.opendatasoft.com/api/records/1.0/search/?dataset=crime-incident-reports-2017%40boston&facet=occurred_on_date&facet=offense_description&facet=street&facet=shooting&facet=offense_code_group&facet=district&facet=reporting_area&facet=location'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        # print(type(response))
        r = json.loads(response)
        # print(type(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(type(s))
        repo.dropCollection("CrimeData")
        repo.createCollection("CrimeData")
        repo['adsouza_mcsmocha.CrimeData'].insert_one(r)
        repo['adsouza_mcsmocha.CrimeData'].metadata({'complete':True})
        print(repo['adsouza_mcsmocha.CrimeData'].metadata())

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

        this_script = doc.agent('alg:adsouza_mcsmocha#requestCrimeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('ods:crime-incident-reports-2017@boston', {'prov:label':'Crime Data, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval',
        	'ont:Query':'?type=Crime+Data&$occured_on_date,offense_description,street,ahooting,offense_code_group,district,reporting_area,location'
        	}
        	)

        crime = doc.entity('dat:adsouza_mcsmocha#CrimeData', {prov.model.PROV_LABEL:'Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc

requestCrimeData.execute()
doc = requestCrimeData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))