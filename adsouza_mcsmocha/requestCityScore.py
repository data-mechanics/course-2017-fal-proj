import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class requestCityScore(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = []
    writes = ['adsouza_mcsmocha.CityScore']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        url = 'https://data.boston.gov/export/5bc/e8e/5bce8e71-5192-48c0-ab13-8faff8fef4d7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace("]", "")
        response += "]"
        # print(type(response))
        r = json.loads(response)
        # print(type(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(type(s))
        repo.dropCollection("CityScore")
        repo.createCollection("CityScore")
        repo['adsouza_mcsmocha.CityScore'].insert_many(r)
        repo['adsouza_mcsmocha.CityScore'].metadata({'complete':True})
        print(repo['adsouza_mcsmocha.CityScore'].metadata())

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

        this_script = doc.agent('alg:adsouza_mcsmocha#requestCityScore', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('anb:5bce8e71-5192-48c0-ab13-8faff8fef4d7', {'prov:label':'CityScore, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_city = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_city, this_script)
        doc.usage(get_city, resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval',
        	'ont:Query':'?type=CityScore&$CTY_SCR_NAME,CTY_SCR_NBR_QT_01,CTY_SCR_TGT_01'
        	}
        	)

        city_score = doc.entity('dat:adsouza_mcsmocha#CityScore', {prov.model.PROV_LABEL:'CityScore', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(city_score, this_script)
        doc.wasGeneratedBy(city_score, get_city, endTime)
        doc.wasDerivedFrom(city_score, resource, get_city, get_city, get_city)

        repo.logout()

        return doc

requestCityScore.execute()
doc = requestCityScore.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))