import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class bikeNetwork():
    # contributor = 'test'
    # reads = []
    # writes = ['test.trafficSignal']
    # contributor = 'alice_bob'
    # reads = []
    # writes = ['alice_bob.lost', 'alice_bob.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        features=r['features']

        features = [


                {'Data': 'Bike Routes','Location':dict['properties']['STREET_NAM'], 'Coordinates': dict['geometry']['coordinates']}

              for dict in features
        ]

        repo.dropCollection("bikeNetwork")
        repo.createCollection("bikeNetwork")
        repo['alanbur_jcaluag.bikeNetwork'].insert_many(features)
        repo['alanbur_jcaluag.bikeNetwork'].metadata({'complete':True})
        repo.logout()
        print(repo['alanbur_jcaluag.bikeNetwork'].metadata())

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

bikeNetwork.execute()