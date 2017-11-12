
import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import sys


class road_connections_with_routes(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.road_segs', 'bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate']
    writes = ['bkin18_cjoe_klovett_sbrz.road_connections_with_routes']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo

        roads_collection = db['bkin18_cjoe_klovett_sbrz.road_segs']
        roads = roads_collection.find()

        routes_collection = db['bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate']
        routes = routes_collection.find()

        # Obtains all roads in the Boston region, that have at least some associated street name data, and removes some entries.

        modifiedDictionary = []

        for route in routes:
            for street in route['INTERSECTIONS']:
                modifiedPiece = {"ST_NAME": street, "NUM_INTERSECTIONS": 1, "BELONGS_TO_ROUTE": route['RT_NAME']}
                modifiedDictionary.append(modifiedPiece)

        finalDictionary = []

        i = 0

        for piece in modifiedDictionary:

            num_intersections = 0
            route_list = []

            for duplicate in modifiedDictionary:
                if(duplicate['ST_NAME'] == piece['ST_NAME'] and duplicate['BELONGS_TO_ROUTE'] not in route_list):
                    num_intersections += 1
                    route_list += [duplicate['BELONGS_TO_ROUTE']]

            unique = 1

            for uniquePiece in finalDictionary:
                if (piece['ST_NAME'] == uniquePiece['ST_NAME']):
                    unique = 0

            finalPiece = {"ST_NAME": piece['ST_NAME'], "NUM_INTERSECTIONS": num_intersections, "ROUTES": route_list}
            if (finalPiece not in finalDictionary):
                print(finalPiece)
                finalDictionary.append(finalPiece)
            i += 1
            if (i == 1000):
                break

        repo.dropCollection("bkin18_cjoe_klovett_sbrz.road_connections_with_routes")
        repo.createCollection("bkin18_cjoe_klovett_sbrz.road_connections_with_routes")
        repo['bkin18_cjoe_klovett_sbrz.road_connections_with_routes'].insert_many(finalDictionary)
        repo['bkin18_cjoe_klovett_sbrz.road_connections_with_routes'].metadata({'complete': True})

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
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://gis.massdot.state.ma.us/arcgis/rest/services/')

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#retrieveRoadsInventory',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:Roads/RoadInventory/MapServer/',
                              {'prov:label': 'Roads Inventory', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_property_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_property_data, this_script)
        doc.usage(get_property_data, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?where=1%3D1&outFields=*&outSR=4326&f=json'
                   }
                  )

        property_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#roads_inventory',
                                 {prov.model.PROV_LABEL: 'roads_inventory', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(property_db, this_script)
        doc.wasGeneratedBy(property_db, get_property_data, endTime)
        doc.wasDerivedFrom(property_db, resource, get_property_data)

        repo.logout()

        return doc