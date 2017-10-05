import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class roadComplaints(dml.Algorithm):
    '''
    extract road complaint data from json file
    '''
    contributor = 'alanbur_jcaluag'
    reads = []
    writes = ['alanbur_jcaluag.roadComplaints']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5bed19f1f9cb41329adbafbd8ad260e5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        features=r['features']

        s = json.dumps(features, sort_keys=True, indent=2)

        # features = [

        #     {'Data': 'Road Complaints',
        #         'Latitude': dict['geometry']['coordinates'][0],
        #         'Longitude': dict['geometry']['coordinates'][1]}
        #       for dict in features
        # ]


        repo.dropCollection("roadComplaints")
        repo.createCollection("roadComplaints")
        repo['alanbur_jcaluag.roadComplaints'].insert_many(features)
        repo['alanbur_jcaluag.roadComplaints'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.roadComplaints'].metadata())
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

        this_script = doc.agent('alg:alanbur_jcaluag#roadComplaints', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:5bed19f1f9cb41329adbafbd8ad260e5_0', {'prov:label':'Road Complaints', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_complaints, this_script)
        doc.usage(get_complaints, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        roadComplaints = doc.entity('dat:alanbur_jcaluag#roadComplaints', {prov.model.PROV_LABEL:'Road Complaints', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(roadComplaints, this_script)
        doc.wasGeneratedBy(roadComplaints, get_complaints, endTime)
        doc.wasDerivedFrom(roadComplaints, resource, get_complaints, get_complaints, get_complaints)

        repo.logout()
                  
        return doc
# roadComplaints.execute()