
import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import sys
import pdb
from z3 import *
import argparse



def find_streets(num_roads):
    """
    Takes an integer input and returns a z3 computation to get emergency routes equal to integer
    that satisfy the constraints.
    """

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

    # Constraints for each emergency route within the data set # Must be either 0 or 1
    for i in range(len(e_traffic_aggregate)):
        key = e_traffic_aggregate[i]['RT_NAME']
        key = key.replace('.','')

        eroute_z3.update({key : z3.Int('e'+str(i))})

        z3_eroute.update({'e'+str(i) : key})

        e_route = eroute_z3[key]
        S.add(e_route >= 0, e_route <=1)


    # Constraints for each regular street within the data set
    # Must be greater than 0 and must be equal to the sum of the connected emergency routes
    connects = []
    for i in range(len(road_connections)):
        street = z3.Int('s'+str(i))
        p_connections = road_connections[i]['ROUTES']
        connections = []

        for connection in p_connections:
            connections.append(connection.replace('.',''))

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

    S.add(total == num_roads)

    S.check()
    m = S.model()

    # return used streets
    streets = ""

    for d in m:
        if str(d)[0] == 'e' and m[d] == 1:
            streets = streets + z3_eroute[str(d)] + ", "

    repo.logout()

    return streets[:-2]
