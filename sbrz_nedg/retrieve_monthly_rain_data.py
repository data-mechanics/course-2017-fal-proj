import time
import urllib.request
from bson import json_util
import prov.model
import dml
import datetime
import uuid



class retrieve_monthly_rain_data(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.rainData']
    @staticmethod
    def execute(trial = False):
        '''Retrieve library_visits data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        # library visit Data Set.
        jan_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_01010131/q/MA/Boston.json")).read().decode("utf-8"))
        feb_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_02010228/q/MA/Boston.json")).read().decode("utf-8"))
        mar_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_03010331/q/MA/Boston.json")).read().decode("utf-8"))
        apr_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_04010430/q/MA/Boston.json")).read().decode("utf-8"))
        may_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_05010531/q/MA/Boston.json")).read().decode("utf-8"))
        jun_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_06010631/q/MA/Boston.json")).read().decode("utf-8"))
        time.sleep(60) #database only allows 10 queries a minute, so I put a sleep halfway through
        jul_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_07010731/q/MA/Boston.json")).read().decode("utf-8"))
        aug_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_08010831/q/MA/Boston.json")).read().decode("utf-8"))
        sep_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_09010930/q/MA/Boston.json")).read().decode("utf-8"))
        oct_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_10011031/q/MA/Boston.json")).read().decode("utf-8"))
        nov_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_11011130/q/MA/Boston.json")).read().decode("utf-8"))
        dec_rain_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
           "http://api.wunderground.com/api/40f7f8bd3f0f7fb1/planner_12011231/q/MA/Boston.json")).read().decode("utf-8"))

        repo.dropCollection("rainData")
        repo.createCollection("rainData")
        repo['sbrz_nedg.rainData'].insert_one(jan_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(feb_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(mar_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(apr_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(may_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(jun_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(jul_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(aug_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(sep_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(oct_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(nov_rain_json)
        repo['sbrz_nedg.rainData'].insert_one(dec_rain_json)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('bdp', 'http://api.wunderground.com/api/')


        this_script = doc.agent('alg:sbrz_nedg#retrieve_monthly_rain_data.py', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:40f7f8bd3f0f7fb1', {'prov:label': 'Monthly Weather Data', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        retrieve_monthly_rain_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(retrieve_monthly_rain_data, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': '&limit=200000'})

        weather_db = doc.entity('dat:sbrz_nedg#retrieve_monthly_rain_data.py', {prov.model.PROV_LABEL: 'rainData', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(this_script)
        doc.wasGeneratedBy(retrieve_monthly_rain_data)
        doc.wasDerivedFrom(weather_db, resource)

        repo.logout()

        return doc