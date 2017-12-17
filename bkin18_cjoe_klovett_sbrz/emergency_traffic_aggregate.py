import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import re
import sys

class emergency_traffic_aggregate(dml.Algorithm):
    """
    Aggregates all intersections from the same street
    """
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.emergency_traffic_selection']
    writes = ['bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        print("Aggregating traffic data...            \n", end='\r')
        sys.stdout.write("\033[F") # Cursor up one line

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz') # should probably move this to auth

        # Build our lists
        traffic_selection = [x for x in repo['bkin18_cjoe_klovett_sbrz.emergency_traffic_selection'].find()]
        traffic_keys, traffic_intersecs, traffic_dicts = [], [], []
        route_names = []
        routes_dict = {}

        for selection in traffic_selection:
            if(selection['emergency_route'] not in traffic_keys):
                traffic_keys.append(selection['emergency_route'].replace('.',''))

        for key in traffic_keys:
            routes_dict.update({key:[]})

        for signals in traffic_selection:
            streets = re.split(", & |, | & | @ ", signals['Location'])

            e_street = ''

            for i in range(len(streets)):
                streets[i] = streets[i].replace('.', '')

            for street in streets:
                if street in traffic_keys:
                    e_street = street
            
            streets.remove(e_street)
            for street in streets:
                if street not in routes_dict[e_street]:
                    routes_dict[e_street].append(street)


        for key in traffic_keys:
            if len(routes_dict[key]) != 0:
                traffic_intersecs.append({'RT_NAME': key, 'INTERSECTIONS': routes_dict[key]})
            else:
                print(key)


        repo.dropCollection("emergency_traffic_aggregate")
        repo.createCollection("emergency_traffic_aggregate")
        repo['bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate'].insert_many(traffic_intersecs)

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
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#emergency_traffic_aggregate', 
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, 
            { prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'.find()'})

        selection_input = doc.entity('dat:bkin18_cjoe_klovett_sbrz.emergency_traffic_selection', 
            { prov.model.PROV_LABEL:'Emergency Traffic Selection', prov.model.PROV_TYPE:'ont:DataSet'})

        output = doc.entity('dat:bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate', 
            { prov.model.PROV_LABEL:'Emergency Traffic Aggregation', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, selection_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, selection_input, this_run, this_run, this_run)

        repo.logout()

        return doc

## eof
