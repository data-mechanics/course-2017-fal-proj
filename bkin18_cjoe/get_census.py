import urllib.request
import json
from json import loads, dumps
from collections import OrderedDict
import dml
import prov.model
import datetime
import uuid
import pdb
import csv

def convert_census_csv():
    url = 'http://datamechanics.io/data/bkin18_cjoe/Blocks_Boston_2010_BARI%20CSV.csv'
    csvfile = urllib.request.urlopen(url).read().decode("utf-8")

    dict_values = []

    entries = csvfile.split('\n')
    keys = entries[0].split(',')
    keys[-1] = keys[-1][:-1]

    count = 0

    for row in entries[1:-1]:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
        dict_values.append(dictionary)

    return dict_values


class get_census(dml.Algorithm):
    contributor = 'bkin18_cjoe'
    reads = []
    writes = ['bkin18_cjoe.census']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe', 'bkin18_cjoe') # should probably move this to auth

        dict_values = convert_census_csv()

        repo.dropCollection("census")
        repo.createCollection("census")
        repo['bkin18_cjoe.census'].insert_many(dict_values)

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

        this_script = doc.agent('alg:bkin18_cjoe#get_census', 
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, 
            { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        census_input = doc.entity('hdv:DVN_FI1YED', 
            { 'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv', 'ont:Query':'??persistentId=doi:10.7910'})
        
        output = doc.entity('dat:bkin18_cjoe#census', 
            { prov.model.PROV_LABEL:'Census', prov.model.PROV_TYPE:'ont:DataSet'})
    
        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, census_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, census_input, this_run, this_run, this_run)

        repo.logout()

        return doc


get_census.execute()
doc = get_census.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

