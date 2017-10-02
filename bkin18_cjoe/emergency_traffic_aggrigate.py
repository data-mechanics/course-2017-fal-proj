import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import re

class emergency_traffic_aggrigate(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = ['bkin18_cjoe.emergency_traffic_selection']
    writes = ['bkin18_cjoe.emergency_traffic_aggrigate']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe') # should probably move this to auth

        traffic_selection = [x for x in repo['bkin18_cjoe.emergency_traffic_selection'].find()]

        traffic_keys, traffic_intersecs, traffic_dicts = [], [], []

        for signals in traffic_selection:
            streets = re.split(", & |, | & | @ ", signals['Location'])
            traffic_intersecs.append(streets)
            street_check = streets[0]
            if(street_check[-1] == '.'):
                    street_check = street_check[:-1]
            pdb.set_trace()
            if(street_check not in traffic_keys):
                traffic_keys.append(street_check)


        # This is terrible, gotta fix it later
        for i in range(len(traffic_keys)):
            key_name = traffic_keys[i]
            traffic_dicts.append({key_name:[]})
            for j in range(len(traffic_intersecs)):
                if traffic_intersecs[j][0] == key_name:
                    for k in range(1, len(traffic_intersecs[j])):
                        if(traffic_intersecs[j][k] not in traffic_dicts[i][key_name]): 
                            traffic_dicts[i][key_name].append(traffic_intersecs[j][k])

        #pdb.set_trace()


        repo.dropCollection("emergency_traffic_aggrigate")
        repo.createCollection("emergency_traffic_aggrigate")
        repo['bkin18_cjoe.emergency_traffic_aggrigate'].insert_many(traffic_dicts)


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


        this_script = doc.agent('alg:bkin18_cjoe#emergency_traffic_aggrigate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        #resource = doc.entity('bdp:4f3e4492e36f4907bcd307b131afe4a5_0',
        #    {'prov:label':'311, Service Requests',
        #    prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'}) 

        ## Work on this later
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        route_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        routes = doc.entity('dat:bkin18_cjoe#emergency_traffic_aggrigate', {prov.model.PROV_LABEL:'Emergency Traffic Selection', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(route_activity, this_script)
        #doc.usage(route_activity, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        #doc.wasAttributedTo(found, this_script)

        doc.wasAttributedTo(routes, this_script)
        doc.wasGeneratedBy(routes, route_activity, endTime)
        #doc.wasDerivedFrom(routes, resource, this_run, this_run, this_run)


        repo.logout()


        return doc


emergency_traffic_aggrigate.execute()
doc = emergency_traffic_aggrigate.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

