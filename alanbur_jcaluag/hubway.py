import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hubway():
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.hubway']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'https://secure.thehubway.com/data/stations.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        stations=r['stations']


        stations = [
            {'Data': 'Hubway Stations',
            'Location':dict['s'],
            'Latitude': dict['la'],
            'Longitude': dict['lo']}
            for dict in stations]

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        repo['alanbur_jcaluag.hubway'].insert_many(stations)
        repo['alanbur_jcaluag.hubway'].metadata({'complete':True})
        repo.logout()
        print(repo['alanbur_jcaluag.hubway'].metadata())

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

# hubway.execute()