import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import pdb
import json
import sys

class wazeAggregates(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.waze_data'] 
            
    writes = ['bkin18_cjoe_klovett_sbrz.waze_levels_agg',
            'bkin18_cjoe_klovett_sbrz.waze_speeds_agg']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        print("Aggregating waze data...         \n", end='\r')
        sys.stdout.write("\033[F") # Cursor up one line


        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')

        data = [x for x in repo['bkin18_cjoe_klovett_sbrz.waze_data'].find()]


        # Create two seperate aggrigations from waze data:
        # Aggregated by street, one for traffic jam level and one for speed 
        traffic_data = []

        for entry in data:
            try:
                traffic_data.append((entry['street'], entry['level'], entry['speed']))

            except KeyError:
                print("")


        streets = []
        levels_agg, speeds_agg = {}, {}

        for jam in traffic_data:
            if jam[0] not in streets:
                streets.append(jam[0])

        
        for street in streets:
            levels_agg.update({street:[]})
            speeds_agg.update({street:[]})

        
        for entry in traffic_data:
            levels_agg[entry[0]].append(entry[1])
            speeds_agg[entry[0]].append(entry[2])

        
        #levels_agg = json.dumps(levels_agg)
        #speeds_agg = json.dumps(speeds_agg) 

        repo.dropCollection("waze_levels_agg")
        repo.createCollection("waze_levels_agg")
        repo['bkin18_cjoe_klovett_sbrz.waze_levels_agg'].insert_one(levels_agg)
        repo['bkin18_cjoe_klovett_sbrz.waze_levels_agg'].metadata({'complete': True})
        # print(repo['bkin18_cjoe_klovett_sbrz.waze_levels_agg'].metadata())

        repo.dropCollection("waze_speeds_agg")
        repo.createCollection("waze_speeds_agg")
        repo['bkin18_cjoe_klovett_sbrz.waze_speeds_agg'].insert_one(levels_agg)
        repo['bkin18_cjoe_klovett_sbrz.waze_speeds_agg'].metadata({'complete': True})
        # print(repo['bkin18_cjoe_klovett_sbrz.waze_speeds_agg'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/')
        doc.add_namespace('cbdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent(
            'alg:bkin18_cjoe_klovett_sbrz#wazeAggregates',{
                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],'ont:Extension': 'py'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        selection_input = doc.entity('dat:bkin18_cjoe_klovett_sbrz.waze_data',
            { prov.model.PROV_LABEL:'Waze Data', prov.model.PROV_TYPE:'ont:DataSet'})

        output1 = doc.entity('dat:bkin18_cjoe_klovett_sbrz.waze_levels_agg', 
            { prov.model.PROV_LABEL:'Emergency Traffic Aggregation', prov.model.PROV_TYPE:'ont:DataSet'})

        output2 = doc.entity('dat:bkin18_cjoe_klovett_sbrz.waze_speeds_agg', 
            { prov.model.PROV_LABEL:'Emergency Traffic Aggregation', prov.model.PROV_TYPE:'ont:DataSet'})


        doc.wasAssociatedWith(this_run , this_script)

        doc.used(this_run, selection_input, startTime)

        waze_levels_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#waze_levels_agg', 
                {prov.model.PROV_LABEL: 'waze_levels_agg', prov.model.PROV_TYPE: 'ont:DataSet'})

        waze_speeds_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#waze_speeds_agg', 
                {prov.model.PROV_LABEL: 'waze_speeds_agg', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasGeneratedBy(output1, this_run, endTime)
        doc.wasGeneratedBy(output2, this_run, endTime)

        doc.wasDerivedFrom(output1, selection_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output2, selection_input, this_run, this_run, this_run)

        
        doc.wasAttributedTo(waze_levels_db, this_script)
        doc.wasAttributedTo(waze_speeds_db, this_script)
        
        doc.wasGeneratedBy(waze_levels_db, this_run, endTime)
        doc.wasGeneratedBy(waze_speeds_db, this_run, endTime)

        repo.logout()

        return doc

