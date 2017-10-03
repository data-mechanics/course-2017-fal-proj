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
    csvfile = open('Blocks_Boston_2010_BARI CSV.csv', 'r')
    jsonfile = open('Census 2010.json', 'w')

    fieldnames = ("STATEFP10","COUNTYFP10","TRACTCE10","BLOCKCE10","GEOID10","NAME10","MTFCC10","ALAND10","AWATER10","INTPTLAT10","INTPTLON10","POP100_RE","HU100_RE","BG_ID_10","CT_ID_10","BOSNA_R_ID","NSA_NAME","BRA_PD_ID","BRA_PD","ZIPCODE","City_Counc","WARD","PRECINCTS","ISD_Nbhd","Police_Dis","Fire_Distr","PWD","Blk_ID_10")
    reader = csv.DictReader(csvfile, fieldnames)
    count = 0
    for row in reader:
        if count > 1:
            count += 1
            # print("paspaspaspaspapsaspp")
            pass
        json.dump(row, jsonfile)
        jsonfile.write('\n')


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

        convert_census_csv()

        dl = []
        for line in open('Census 2010.json'):

            dl.append(line)

        for i in range(len(dl)):
            dl[i] = json.loads(dl[i])

        # print(dl[3])
        # print(type(dl[3]))
        # print(dl)
        # rjson.load(dl)

        repo.dropCollection("census")
        repo.createCollection("census")
        repo['bkin18_cjoe.census'].insert_many(dl)

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


        this_script = doc.agent('alg:bkin18_cjoe#get_census', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ## I don't actually know what the key is to this (bdp:wc8w-nujj)
        resource = doc.entity('bdp:DVN_FI1YED', { 'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        ## Work on this later
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        route_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        routes = doc.entity('dat:bkin18_cjoe#census', {prov.model.PROV_LABEL:'Census', prov.model.PROV_TYPE:'ont:DataSet'})
    
        doc.wasAssociatedWith(route_activity, this_script)
        doc.usage(route_activity, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        # doc.wasAttributedTo(found, this_script)

        doc.wasAttributedTo(routes, this_script)
        doc.wasGeneratedBy(routes, route_activity, endTime)
        doc.wasDerivedFrom(routes, resource, this_run, this_run, this_run)

        # emergency_routes = doc.entity('bdp:4f3e4492e36f4907bcd307b131afe4a5_0',
        #     {'prov:label':'311, Service Requests',
        #     prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension':'geojson'}) 

        repo.logout()


        return doc


get_census.execute()
doc = get_census.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

