import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class stopsData():
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.hubwayFiltered', 'alanbur_jcaluag.mbtaProjected']
    writes = ['alanbur_jcaluag.stopsData']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        hubwayCollection=repo['alanbur_jcaluag.hubwayFiltered'].find()
        mbtaCollection =repo['alanbur_jcaluag.mbtaProjected'].find()
        mbta = [x for x in mbtaCollection]
        hubway = [y for y in hubwayCollection]


        DSet = hubway + mbta

        repo.dropCollection("stopsData")
        repo.createCollection("stopsData")
        repo['alanbur_jcaluag.stopsData'].insert_many(DSet)
        repo['alanbur_jcaluag.stopsData'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.stopsData'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

#stopsData.execute()