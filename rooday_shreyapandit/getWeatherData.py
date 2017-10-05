import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getWeatherData(dml.Algorithm):
    contributor = 'rooday_shreyapandit'
    reads = []
    writes = ['rooday_shreyapandit.weather']

    @staticmethod
    def execute(trial = False):
        # Retrieve some data sets (not using the API here for the sake of simplicity).
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        url = 'http://datamechanics.io/data/rooday_shreyapandit/storm_data_search_results.json'
        resp = requests.get(url).json()

        print("Weather response has come, inserting....")

        repo.dropCollection("weather")
        repo.createCollection("weather")
        repo['rooday_shreyapandit.weather'].insert_many(resp)
        repo['rooday_shreyapandit.weather'].metadata({'complete':True})

        print("response has been inserted")
        print(repo['rooday_shreyapandit.weather'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()
        print("Done!")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("***Doc is")
        print(doc)
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/rooday_shreyapandit') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/rooday_shreyapandit') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('weather', 'http://datamechanics.io/data/rooday_shreyapandit/')

        this_script = doc.agent('alg:#getWeatherData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:storm_data_search_results.json', {'prov:label':'Inclement Weather Data for Boston and Suffolk', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_weather_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_weather_data, this_script)
        doc.usage(get_weather_data, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        weather_data = doc.entity('dat:#weather', {prov.model.PROV_LABEL:'Inclement Weather Data for Boston and Suffolk', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(weather_data, this_script)
        doc.wasGeneratedBy(weather_data, get_weather_data, endTime)
        doc.wasDerivedFrom(weather_data, resource, get_weather_data, get_weather_data, get_weather_data)

        repo.logout()
                  
        return doc
