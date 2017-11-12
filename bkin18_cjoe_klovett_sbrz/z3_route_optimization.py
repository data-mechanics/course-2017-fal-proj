
import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import sys
import pdb
from z3 import *



class z3_route_optimization(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.road_connections_with_routes']
    writes = ['bkin18_cjoe_klovett_sbrz.z3_route_optimization']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo

        road_connections = list(repo['bkin18_cjoe_klovett_sbrz.road_connections_with_routes'].find())
        e_traffic_aggregate = list(repo['bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate'].find())

        S = Solver()

        street_z3 = {}
        z3_street = {}
        street_vars = []

        total = z3.Int('total')

        # Create constraints for each emergency route within the data set
        # Must be either 0 or 1
        for i in range(len(e_traffic_aggregate)):
            street_z3.update({e_traffic_aggregate[i]['RT_NAME'] : z3.Int('e'+str(i))})

            z3_street.update({'e'+str(i) : e_traffic_aggregate[i]['RT_NAME']})

            e_route = street_z3[e_traffic_aggregate[i]['RT_NAME']]
            S.add(e_route >= 0, e_route <=1)

        connects = []

        for i in range(len(road_connections)):
            street = z3.Int('s'+str(i))
            connections = road_connections[i]['ROUTES']

            for j in range(len(connections)):
                connects.append(street_z3[connections[j]])

            S.add(street > 0)
            S.add(street <= Sum(connects))

        for key, value in street_z3.items():
            street_vars.append(value)

        S.add(total < Sum(street_vars))

        print(S.check())
        m = S.model()

        for d in m: print("%s -> %s" % (d, m[d]))

        pdb.set_trace()

        #repo.dropCollection("bkin18_cjoe_klovett_sbrz.z3_route_optimization")
        #repo.createCollection("bkin18_cjoe_klovett_sbrz.z3_route_optimization")
        #repo['bkin18_cjoe_klovett_sbrz.z3_route_optimization'].insert_many(finalDictionary)
        #repo['bkin18_cjoe_klovett_sbrz.z3_route_optimization'].metadata({'complete': True})

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


test = z3_route_optimization()
test.execute()
