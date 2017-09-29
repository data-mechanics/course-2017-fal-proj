import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getColleges(dml.Algorithm):
    contributor = 'jdbrawn_slarbi'
    reads = []
    writes = ['jdbrawn_slarbi.colleges']

    @staticmethod
    def execute(trial = False):
        """Retrieve the college location data from Analyze Boston"""
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        url = 'https://data.boston.gov/datastore/odata3.0/208dc980-a278-49e3-b95b-e193bb7bb6e4?$top=65&$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("colleges")
        repo.createCollection("colleges")
        repo['jdbrawn_slarbi.colleges'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}