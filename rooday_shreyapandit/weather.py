import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class weather(dml.Algorithm):
    contributor = 'rooday_shreyapandit'
    reads = []
    writes = ['rooday_shreyapandit.weather']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        #Get 311 data
#        url = 'https://data.boston.gov/export/296/8e2/2968e2c0-d479-49ba-a884-4ef523ada3c0.json'
        url = 'http://datamechanics.io/data/rooday_shreyapandit/storm_data_search_results.json'

        # response = urllib.request.urlopen(url).read().decode("utf-8")
        r = requests.get(url).json()#.read().decode("utf-8")
#        print(response)
        print("response has come, inserting....")
        print(r[1:100])
        #r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("weather")
        repo.createCollection("weather")
        repo['rooday_shreyapandit.weather'].insert_many(r)
        repo['rooday_shreyapandit.weather'].metadata({'complete':True})
        print("response has been inserted")
        print(repo['rooday_shreyapandit.weather'].metadata())

        # url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("found")
        # repo.createCollection("found")
        # repo['rooday_shreyapandit.found'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()
        print("Done!")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')
        # doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        # doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        # doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # this_script = doc.agent('alg:rooday_shreyapandit#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        # resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # doc.wasAssociatedWith(get_found, this_script)
        # doc.wasAssociatedWith(get_lost, this_script)
        # doc.usage(get_found, resource, startTime, None,
        #           {prov.model.PROV_TYPE:'ont:Retrieval',
        #           'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
        #           }
        #           )
        # doc.usage(get_lost, resource, startTime, None,
        #           {prov.model.PROV_TYPE:'ont:Retrieval',
        #           'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #           }
        #           )

        # lost = doc.entity('dat:rooday_shreyapandit#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(lost, this_script)
        # doc.wasGeneratedBy(lost, get_lost, endTime)
        # doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        # found = doc.entity('dat:rooday_shreyapandit#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(found, this_script)
        # doc.wasGeneratedBy(found, get_found, endTime)
        # doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        # repo.logout()
                  
        # return doc

getData.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))