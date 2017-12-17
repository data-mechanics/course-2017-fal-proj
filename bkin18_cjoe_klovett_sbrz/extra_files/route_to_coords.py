import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import csv
import sys

class route_to_coords(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.high_priority_routes']
    writes = ['bkin18_cjoe_klovett_sbrz.route_coordinates']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo

        authData = dml.auth;
        key = authData['services']['googleportal']['key']
        print("KEY:", key)

        baseURL = 'https://maps.googleapis.com/maps/api/geocode/json?address='

        routes = list(repo['bkin18_cjoe_klovett_sbrz.high_priority_routes'].find())[0]['high_priority_routes']
        #print(routes)
        
        
        routeURLs = []

        
        for val in routes:
            routeURL = baseURL + val.replace(" ", "+") + '+Boston+Massachusetts&key=' + key
            routeURLs.append(routeURL)
        
        modifiedDictionary = []

        for i in range(len(routeURLs)):
            url = routeURLs[i]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            modifiedPiece = {'formatted_address': r['results'][0]['formatted_address'], 'geometry': r['results'][0]['geometry']}
            print(r)
            modifiedDictionary.append(modifiedPiece)



        repo.dropCollection("bkin18_cjoe_klovett_sbrz.route_coordinates")
        repo.createCollection("bkin18_cjoe_klovett_sbrz.route_coordinates")
        repo['bkin18_cjoe_klovett_sbrz.route_coordinates'].insert_many(modifiedDictionary)
        
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
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('goo', 'https://maps.googleapis.com/maps/api/geocode/json')

        ## Agent
        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#routes_to_coords',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        ## Activity
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime,
            { prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'.find()'})

        ## Entities
        priority_routes = doc.entity('dat:bkin18_cjoe_klovett_sbrz.high_priority_routes',
            { prov.model.PROV_LABEL:'High Priority Routes', prov.model.PROV_TYPE:'ont:DataSet'})

        output = doc.entity('dat:bkin18_cjoe_klovett_sbrz.route_coordinates',
            { prov.model.PROV_LABEL:'Route Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})


        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, priority_routes, startTime)
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, priority_routes, this_run, this_run, this_run)

        repo.logout()

        return doc