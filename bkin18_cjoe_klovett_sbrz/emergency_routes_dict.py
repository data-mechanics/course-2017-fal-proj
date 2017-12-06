import dml
import prov.model
import datetime
import uuid
import sys
import pdb

class emergency_routes_dict(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.emergency_routes']
    writes = ['bkin18_cjoe_klovett_sbrz.emergency_routes_dict']

    @staticmethod
    def execute(trial=False):
        ''' Creates a single dictionary to be used by the server for retrieving 
            google map formated coordinates for specific emergency routes
        '''

        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo

        # Take the emergency routes data set, aggrigate it by street, and then put it
        # in a format which google maps can read
        roads_dict = {}
        google_map_coords, emergency_keys, segment = [], [], []

        emergency_segs = [x for x in db['bkin18_cjoe_klovett_sbrz.emergency_routes'].find()]

        for routes in emergency_segs:
            key = routes['properties']['FULL_NAME'] 
            key = key.replace('.','')
            if(key not in emergency_keys):
                emergency_keys.append(key) 


        for key in emergency_keys:
            roads_dict.update({key:[]})


        for routes in emergency_segs:
            key = routes['properties']['FULL_NAME']
            key = key.replace('.','')

            for coordinate in routes['geometry']['coordinates']:
                segment.append({'lat': coordinate[1], 'lng': coordinate[0]})

            roads_dict[key].append(segment)
            segment = []


        repo.dropCollection('bkin18_cjoe_klovett_sbrz.emergency_routes_dict')
        repo.createCollection('bkin18_cjoe_klovett_sbrz.emergency_routes_dict')
        repo['bkin18_cjoe_klovett_sbrz.emergency_routes_dict'].insert_one(roads_dict)
  
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
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#DataSet')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        ## Agent
        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#emergency_routes_dict',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        ## Activity
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime,
            { prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'.find()'})

        ## Entities
        emergency_routes = doc.entity('dat:bkin18_cjoe_klovett_sbrz.emergency_routes',
            { prov.model.PROV_LABEL:'Route Coordinate List', prov.model.PROV_TYPE:'ont:DataSet'})

        output = doc.entity('dat:bkin18_cjoe_klovett_sbrz.emergency_routes_dict',
            { prov.model.PROV_LABEL:'Formatted Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})


        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, emergency_routes, startTime)

        doc.wasAttributedTo(output, this_script)

        doc.wasGeneratedBy(output, this_run, endTime)

        doc.wasDerivedFrom(output, emergency_routes, this_run, this_run, this_run)

        repo.logout()

        return doc


