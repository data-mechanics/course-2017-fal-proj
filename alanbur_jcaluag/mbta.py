import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta():
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
        api_key='iMJT5dgjok-SFSpHyPT-YQ'
        url1 = 'http://realtime.mbta.com/developer/api/v2/routes?api_key='+api_key+'&format=json'
        response = urllib.request.urlopen(url1).read().decode("utf-8")
        r = json.loads(response)
        print(r)
        stops=[]
        for routeType in r['mode']:
            print(routeType['mode_name'])
            if routeType['mode_name']=='Bus':
                print('busses')
                for route in routeType['route']:
                    url='http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key='+api_key+'&route='+route['route_id']+'&format=json'
                    response = urllib.request.urlopen(url).read().decode("utf-8")
                    r = json.loads(response)
                    # print(r)
                    # print(r['direction'][0]['stop'])
                    stops.extend(r['direction'][0]['stop'])
                    # print(r)
        print(stops)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # print(type(s))
        repo.dropCollection("mbta")
        repo.createCollection("mbta")
        repo['alanbur_jcaluag.mbta'].insert_many(stops)
        repo['alanbur_jcaluag.mbta'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.mbta'].metadata())

    

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

mbta.execute()