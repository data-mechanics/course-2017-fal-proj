
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
    reads = ['bkin18_cjoe_klovett_sbrz.road_connections_with_routes', 'bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate']
    writes = ['bkin18_cjoe_klovett_sbrz.high_priority_routes', 'bkin18_cjoe_klovett_sbrz.low_priority_routes']

    @staticmethod
    def execute(trial=False):
        """
            This algorithm uses the z3 library to prioritize the emergency snow
            routes. A set of emergency routes where all regular streets have
            access to at least one emergency route will be defined as a high
            priority set. We will be taking the smallest high pririorty set and
            storing it in the database, as well as it's complement.
        """

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo

        road_connections = list(repo['bkin18_cjoe_klovett_sbrz.road_connections_with_routes'].find())
        e_traffic_aggregate = list(repo['bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate'].find())

        S = Solver()

        # eroute_z3 stores an emergency route and a corresponding z3 variable, z3_eroute the opposite
        # z3_eroute is used for retrieval after z3 computataion
        eroute_z3, z3_eroute = {}, {}

        total = z3.Int('total')

        # Constraints for each emergency route within the data set
        # Must be either 0 or 1
        for i in range(len(e_traffic_aggregate)):
            eroute_z3.update({e_traffic_aggregate[i]['RT_NAME'] : z3.Int('e'+str(i))})

            z3_eroute.update({'e'+str(i) : e_traffic_aggregate[i]['RT_NAME']})

            e_route = eroute_z3[e_traffic_aggregate[i]['RT_NAME']]
            S.add(e_route >= 0, e_route <=1)


        # Constraints for each regular street within the data set
        # Must be greater than 0 and must be equal to the sum of the connected emergency routes
        connects = []
        for i in range(len(road_connections)):
            street = z3.Int('s'+str(i))
            connections = road_connections[i]['ROUTES']

            for j in range(len(connections)):
                connects.append(eroute_z3[connections[j]])

            S.add(street > 0)
            S.add(street == Sum(connects))
            connects = []


        # Constraint for the total number of emergency routes.
        # Simply the number in the priority set.
        street_vars = []
        for key, value in eroute_z3.items():
            street_vars.append(value)

        S.add(total == Sum(street_vars))



        # Use binary search for find the smallest high priority set possible.
        upper_bound, lower_bound, val = len(street_vars), 0, 0

        check = (upper_bound - lower_bound) // 2
        prev_check = 0

        satisfiers = []

        while(check != upper_bound and check != lower_bound and check != prev_check):
            print("checking " + str(check) + " emergency routes...")
            S.push()
            S.add(total <= check)

            if str(S.check()) == 'unsat':
                lower_bound = check
                prev_check = check
                check = check + ((upper_bound - lower_bound) // 2)
            elif str(S.check()) == 'sat':
                upper_bound = check
                satisfiers.append(check)
                prev_check = check
                check = check - ((upper_bound - lower_bound) // 2)

            S.pop()

        optimal = min(satisfiers)
        print("minimum emergency streets = " + str(optimal))
        S.add(total <= optimal)

        print(S.check())
        m = S.model()


        # Create dictionaries of the high and low priority emergency routes
        # Add them into the database
        high_priority_routes = {'high_priority_routes':[]}
        low_priority_routes = {'low_priority_routes': []}
        street_name = ''

        for d in m:
            if str(d)[0] == 'e' and m[d] == 1:
                street_name = z3_eroute[str(d)]
                high_priority_routes['high_priority_routes'].append(street_name)
            elif str(d)[0] == 'e' and m[d] == 0:
                street_name = z3_eroute[str(d)]
                low_priority_routes['low_priority_routes'].append(street_name)


        repo.dropCollection("bkin18_cjoe_klovett_sbrz.high_priority_routes")
        repo.createCollection("bkin18_cjoe_klovett_sbrz.high_priority_routes")
        repo['bkin18_cjoe_klovett_sbrz.high_priority_routes'].insert_one(high_priority_routes)
        repo['bkin18_cjoe_klovett_sbrz.high_priority_routes'].metadata({'complete': True})


        repo.dropCollection("bkin18_cjoe_klovett_sbrz.low_priority_routes")
        repo.createCollection("bkin18_cjoe_klovett_sbrz.low_priority_routes")
        repo['bkin18_cjoe_klovett_sbrz.low_priority_routes'].insert_one(low_priority_routes)
        repo['bkin18_cjoe_klovett_sbrz.low_priority_routes'].metadata({'complete': True})


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
        doc.add_namespace('bdp', 'http://gis.massdot.state.ma.us/arcgis/rest/services/')

        ## Agent
        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#z3_route_optimization',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        ## Activity
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime,
            { prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'.find()'})

        ## Entities
        routes_to_streets = doc.entity('dat:bkin18_cjoe_klovett_sbrz.emergency_traffic_aggregate',
            { prov.model.PROV_LABEL:'Emergency Traffic Aggregate', prov.model.PROV_TYPE:'ont:DataSet'})

        streets_to_routes = doc.entity('dat:bkin18_cjoe_klovett_sbrz.road_connections_with_routes',
            { prov.model.PROV_LABEL:'Road Connections With Routes', prov.model.PROV_TYPE:'ont:DataSet'})

        output1 = doc.entity('dat:bkin18_cjoe_klovett_sbrz.high_priority_routes',
            { prov.model.PROV_LABEL:'High Priority Routes', prov.model.PROV_TYPE:'ont:DataSet'})

        output2 = doc.entity('dat:bkin18_cjoe_klovett_sbrz.low_priority_routes',
            { prov.model.PROV_LABEL:'Low Priority Routes', prov.model.PROV_TYPE:'ont:DataSet'})


        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, routes_to_streets, startTime)
        doc.used(this_run, streets_to_routes, startTime)

        doc.wasAttributedTo(output1, this_script)
        doc.wasAttributedTo(output2, this_script)

        doc.wasGeneratedBy(output1, this_run, endTime)
        doc.wasGeneratedBy(output2, this_run, endTime)

        doc.wasDerivedFrom(output1, routes_to_streets, this_run, this_run, this_run)
        doc.wasDerivedFrom(output1, streets_to_routes, this_run, this_run, this_run)

        doc.wasDerivedFrom(output2, routes_to_streets, this_run, this_run, this_run)
        doc.wasDerivedFrom(output2, streets_to_routes, this_run, this_run, this_run)

        repo.logout()

        return doc


#test = z3_route_optimization()
#test.execute()
