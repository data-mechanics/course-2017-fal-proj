import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hubway():
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
        repo.authenticate('test', 'test')

        url = 'https://secure.thehubway.com/data/stations.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        print(type(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        print(type(s))
        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        repo['test.hubway'].insert_many(r)
        repo['test.hubway'].metadata({'complete':True})
        print(repo['test.hubway'].metadata())

    

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

hubway.execute()