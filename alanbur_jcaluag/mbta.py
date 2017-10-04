import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta(dml.algorithml):
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.mbta']

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
        stops=[]
        for routeType in r['mode']:
            if routeType['mode_name']=='Bus':
                for route in routeType['route']:
                    url='http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key='+api_key+'&route='+route['route_id']+'&format=json'
                    response = urllib.request.urlopen(url).read().decode("utf-8")
                    r = json.loads(response)
                    # print(r)
                    # print(r['direction'][0]['stop'])
                    stops.extend(r['direction'][0]['stop'])
                    # print(r)

        stops = [
            {'Data': 'MBTA Bus Stops',
            'Location':dict['stop_name'],
            'Latitude':dict['stop_lat'],
             'Longitude':dict['stop_lon']}

         for dict in stops]

        repo.dropCollection("mbta")
        repo.createCollection("mbta")
        repo['alanbur_jcaluag.mbta'].insert_many(stops)
        repo['alanbur_jcaluag.mbta'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.mbta'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://realtime.mbta.com/developer/')

        this_script = doc.agent('alg:alanbur_jcaluag#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_busStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_busStops, this_script)
        doc.usage(get_busStops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'Complex'
                  }
                  )

        mbta = doc.entity('dat:alanbur_jcaluag#mbta', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_busStops, endTime)
        doc.wasDerivedFrom(mbta, resource, get_busStops, get_busStops, get_busStops)

        repo.logout()
                  
        return doc
# mbta.execute()