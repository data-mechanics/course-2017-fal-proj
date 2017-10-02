import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb

class emergency_traffic_selection(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = ['bkin18_cjoe.emergency_routes', 'bkin18_cjoe.traffic_signals']
    writes = ['bkin18_cjoe.emergency_traffic_selection']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe') # should probably move this to auth

        emergency_routes = [x for x in repo['bkin18_cjoe.emergency_routes'].find()]
        traffic_signals = [x for x in repo['bkin18_cjoe.traffic_signals'].find()]

        emergency_keys, traffic_selection = [], []

        for routes in emergency_routes:
            if routes['properties']['FULL_NAME'] not in emergency_keys:
                emergency_keys.append(routes['properties']['FULL_NAME'])


        for key in emergency_keys:
            for signal in traffic_signals:
                if key in signal['properties']['Location']:
                    traffic_selection.append({'Location': signal['properties']['Location']})

        #pdb.set_trace()


        repo.dropCollection("emergency_traffic_selection")
        repo.createCollection("emergency_traffic_selection")
        repo['bkin18_cjoe.emergency_traffic_selection'].insert_many(traffic_selection)


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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')


        this_script = doc.agent('alg:bkin18_cjoe#emergency_traffic_selection', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        #resource = doc.entity('bdp:4f3e4492e36f4907bcd307b131afe4a5_0',
        #    {'prov:label':'311, Service Requests',
        #    prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'}) 

        ## Work on this later
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        route_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        routes = doc.entity('dat:bkin18_cjoe#emergency_traffic_selection', {prov.model.PROV_LABEL:'Emergency Traffic Selection', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(route_activity, this_script)
        #doc.usage(route_activity, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        #doc.wasAttributedTo(found, this_script)

        doc.wasAttributedTo(routes, this_script)
        doc.wasGeneratedBy(routes, route_activity, endTime)
        #doc.wasDerivedFrom(routes, resource, this_run, this_run, this_run)


        repo.logout()


        return doc


emergency_traffic_selection.execute()
doc = emergency_traffic_selection.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

