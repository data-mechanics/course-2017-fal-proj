import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class projectData(dml.Algorithm):
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.trafficSignal', 'alanbur_jcaluag.mbta', 'alanbur_jcaluag.hubway']

    writes = ['alanbur_jcaluag.trafficSignalProjected','alanbur_jcaluag.mbtaProjected', 'alanbur_jcaluag.hubwayProjected']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        DSet=[]


        collection=repo['alanbur_jcaluag.trafficSignal'].find()

        DSet=[
            {'Dataset': 'Traffic Signals',
                'Location':item['properties']['Location'],
             'Latitude': item['geometry']['coordinates'][0],
             'Longitude': item['geometry']['coordinates'][1]}
              for item in collection
        ]
        repo.dropCollection("trafficSignalFiltered")
        repo.createCollection("trafficSignalFiltered")
        repo['alanbur_jcaluag.trafficSignalFiltered'].insert_many(DSet)
        repo['alanbur_jcaluag.trafficSignalFiltered'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.trafficSignalFiltered'].metadata())
        
        collection=repo['alanbur_jcaluag.hubway'].find()
        DSet=[
            {'DataSet': 'Hubway Stations',
                'Location':item['s'],
                'Latitude': item['la'],
                'Longitude': item['lo']}
              for item in collection
        ]
        print(DSet)
        repo.dropCollection("hubwayProjected")
        repo.createCollection("hubwayProjected")
        repo['alanbur_jcaluag.hubwayProjected'].insert_many(DSet)
        repo['alanbur_jcaluag.hubwayProjected'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.hubwayProjected'].metadata())
        

        collection=repo['alanbur_jcaluag.mbta'].find()
        DSet=[
           {'Data': 'MBTA Bus Stops',
            'Location':item['stop_name'],
            'Latitude':item['stop_lat'],
             'Longitude':item['stop_lon']}
              for item in collection
        ]
        repo.dropCollection("mbtaProjected")
        repo.createCollection("mbtaProjected")
        repo['alanbur_jcaluag.mbtaProjected'].insert_many(DSet)
        repo['alanbur_jcaluag.mbtaProjected'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.mbtaProjected'].metadata())



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
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:alanbur_jcaluag#trafficSignalProjected', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_filter = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_filter, this_script)
        doc.usage(get_filter, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        projected = doc.entity('dat:alanbur_jcaluag#trafficSignalFiltered', {prov.model.PROV_LABEL:'Filtered Traffic Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(projected, this_script)
        doc.wasGeneratedBy(projected, get_filter, endTime)
        doc.wasDerivedFrom(projected, resource, get_filter, get_filter, get_filter)

        repo.logout()
                  
        return doc
    
    
#projectData.execute()