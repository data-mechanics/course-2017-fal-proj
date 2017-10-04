import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class trafficSignal():
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.trafficSignal']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        features=r['features']
        s = json.dumps(r, sort_keys=True, indent=2)

        features = [

            [{'Location':dict['properties']['Location']},
             {'Latitude': dict['geometry']['coordinates'][0]},
             {'Longitude': dict['geometry']['coordinates'][1]}

             ] for dict in features
        ]

        for x in features:
            print(x,'\n')



        repo.dropCollection("trafficSignal")
        repo.createCollection("trafficSignal")
        repo['alanbur_jcaluag.trafficSignal'].insert_many(features)
        repo['alanbur_jcaluag.trafficSignal'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.mbta'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

trafficSignal.execute()