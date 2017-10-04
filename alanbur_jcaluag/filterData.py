import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class filterData():
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.hubwayProjected']
    writes = ['alanbur_jcaluag.hubwayFiltered']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        collection=repo['alanbur_jcaluag.hubwayProjected'].find()

        #Boston Coordinate range
        northLat = 42.392459
        southLat = 42.243896
        eastLong = -71.03568
        westLong = -71.187272

        DSet = [x for x in collection]


        DSet = [x for x in DSet if ((southLat < x['Latitude'] < northLat)) and ((eastLong > x['Longitude'] > westLong))]


        repo.dropCollection("hubwayFiltered")
        repo.createCollection("hubwayFiltered")
        repo['alanbur_jcaluag.hubwayFiltered'].insert_many(DSet)
        repo['alanbur_jcaluag.hubwayFiltered'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.hubwayFiltered'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

#filterData.execute()