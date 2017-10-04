import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class roadComplaints():
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.roadComplaints']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5bed19f1f9cb41329adbafbd8ad260e5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        features=r['features']

        s = json.dumps(features, sort_keys=True, indent=2)

        features = [

            {'Data': 'Road Complaints',
                'Latitude': dict['geometry']['coordinates'][0],
                'Longitude': dict['geometry']['coordinates'][1]}
              for dict in features
        ]


        repo.dropCollection("roadComplaints")
        repo.createCollection("roadComplaints")
        repo['alanbur_jcaluag.roadComplaints'].insert_many(features)
        repo['alanbur_jcaluag.roadComplaints'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.roadComplaints'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

# roadComplaints.execute()