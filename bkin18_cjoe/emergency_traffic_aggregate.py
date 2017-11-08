import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import re

class emergency_traffic_aggregate(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = ['bkin18_cjoe.emergency_traffic_selection']
    writes = ['bkin18_cjoe.emergency_traffic_aggregate']


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

            for i in range(len(streets)):
                streets[i] = streets[i].replace('.', '')

            traffic_intersecs.append(streets)

            if(streets[0] not in traffic_keys):
                traffic_keys.append(streets[0])


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


        repo.dropCollection("emergency_traffic_aggregate")
        repo.createCollection("emergency_traffic_aggregate")
        repo['bkin18_cjoe.emergency_traffic_aggregate'].insert_many(traffic_dicts)


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
        doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:bkin18_cjoe#emergency_traffic_aggregate', 
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, 
            { prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'.find()'})

        selection_input = doc.entity('dat:bkin18_cjoe.emergency_traffic_selection', 
            { prov.model.PROV_LABEL:'Emergency Traffic Selection', prov.model.PROV_TYPE:'ont:DataSet'})

        output = doc.entity('dat:bkin18_cjoe.emergency_traffic_aggregate', 
            { prov.model.PROV_LABEL:'Emergency Traffic Aggregation', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, selection_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, selection_input, this_run, this_run, this_run)

        repo.logout()

        return doc

emergency_traffic_aggregate.execute()
doc = emergency_traffic_aggregate.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

