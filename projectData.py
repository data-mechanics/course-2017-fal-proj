import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class projectData():
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.trafficSignal']
    writes = ['alanbur_jcaluag.trafficSignalFiltered']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        trafficSignal=[]
        collection=repo['alanbur_jcaluag.trafficSignal'].find()
        trafficSignal=[
            {'Dataset': 'Traffic Signals',
                'Location':item['properties']['Location'],
             'Latitude': item['geometry']['coordinates'][0],
             'Longitude': item['geometry']['coordinates'][1]}
              for item in collection
        ]

        repo.dropCollection("trafficSignalFiltered")
        repo.createCollection("trafficSignalFiltered")
        repo['alanbur_jcaluag.trafficSignalFiltered'].insert_many(trafficSignal)
        repo['alanbur_jcaluag.trafficSignalFiltered'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.trafficSignalFiltered'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

projectData.execute()