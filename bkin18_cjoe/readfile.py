import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class readfile(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = []
    writes = ['bkin18_cjoe.neighborhood'] # can add more if need be (e.g. lost, found)

    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('alice_bob', 'alice_bob')

    @staticmethod 
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe') # should probably move this to auth

        # Opening up json
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        print(type(r), type(s))
        # print("jaefoijefoai")
        repo.dropCollection("neighborhood")
        repo.createCollection("neighborhood")
        repo['bkin18_cjoe.neighborhood'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe')
        return 