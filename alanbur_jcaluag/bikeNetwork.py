import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class bikeNetwork():
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.bikeNetwork']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        features=r['features']

        features = [


                {'Data': 'Bike Routes','Location':dict['properties']['STREET_NAM'], 'Coordinates': dict['geometry']['coordinates']}

              for dict in features
        ]

        repo.dropCollection("bikeNetwork")
        repo.createCollection("bikeNetwork")
        repo['alanbur_jcaluag.bikeNetwork'].insert_many(features)
        repo['alanbur_jcaluag.bikeNetwork'].metadata({'complete':True})
        repo.logout()
        print(repo['alanbur_jcaluag.bikeNetwork'].metadata())

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
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:alanbur_jcaluag#bikeNetwork', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bikeNetwork = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bikeNetwork, this_script)
        doc.usage(get_bikeNetwork, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
                  }
                  )

        bikeNetwork = doc.entity('dat:alanbur_jcaluag#bikeNetwork', {prov.model.PROV_LABEL:'Bike Network Paths', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeNetwork, this_script)
        doc.wasGeneratedBy(bikeNetwork, get_bikeNetwork, endTime)
        doc.wasDerivedFrom(bikeNetwork, resource, get_bikeNetwork, get_bikeNetwork, get_bikeNetwork)

        repo.logout()
                  
        return doc
# bikeNetwork.execute()