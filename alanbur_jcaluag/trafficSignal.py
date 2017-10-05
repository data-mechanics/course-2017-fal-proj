import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class trafficSignal(dml.Algorithm):
    '''
    extract traffic signal data from url
    '''
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.trafficSignal']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        features=r['features']
        s = json.dumps(r, sort_keys=True, indent=2)

      


        repo.dropCollection("trafficSignal")
        repo.createCollection("trafficSignal")
        repo['alanbur_jcaluag.trafficSignal'].insert_many(features)
        repo['alanbur_jcaluag.trafficSignal'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.trafficSignal'].metadata())
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
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:alanbur_jcaluag#trafficSignal', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:de08c6fe69c942509089e6db98c716a3_0', {'prov:label':'Traffic Signal Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_trafficSig = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_trafficSig, this_script)
        doc.usage(get_trafficSig, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        trafficSignal = doc.entity('dat:alanbur_jcaluag#trafficSignal', {prov.model.PROV_LABEL:'Traffic Signal Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficSignal, this_script)
        doc.wasGeneratedBy(trafficSignal, get_trafficSig, endTime)
        doc.wasDerivedFrom(trafficSignal, resource, get_trafficSig, get_trafficSig, get_trafficSig)

        repo.logout()
                  
        return doc


# trafficSignal.execute()