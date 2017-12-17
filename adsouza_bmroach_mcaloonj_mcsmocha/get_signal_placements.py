"""
Filename: get_signal_placements.py

Last edited by: BMR 11/28/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Original skeleton files provided by Andrei Lapets (lapets@bu.edu)

Development Notes:

"""

from sklearn.cluster import KMeans
from scipy.cluster.vq import kmeans, vq
import numpy as np
import json
from scipy import stats
import datetime
import itertools
import collections
import dml
import prov.model
import urllib.request
import uuid
from geopy.distance import vincenty


class get_signal_placements(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = ['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers', 'adsouza_bmroach_mcaloonj_mcsmocha.nodes']
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements']

        @staticmethod
        def execute(trial=False, logging=True, sign_count=30, buffer_size=.5):
            startTime = datetime.datetime.now()

            """
            Parameters            
            - sign_count varies the number of signs which can be placed
            - buffer_size disallows signs to be placed within this radius (in miles) from already placed signs
              Use caution increasing this too much - it may cause the inability to find a candidate intersection
            """
            if trial:
                sign_count = 5
                # ^ you'll disallow signs to be placed if you alter this too much. Smaller area, fewer signs.
                # It'll soft fail via a caught exception, but still not ideal

            if logging:
                print("in get_signal_placements.py")

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
            repo.dropCollection("signal_placements")
            repo.createCollection("signal_placements")

            input_data = repo['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers'].find()[0]
            triggers = []

            for field, coords in input_data.items():
                if field != '_id':
                    for crd in coords:
                        triggers.append(crd)

            X =  np.array(triggers)

            kmeans_output = KMeans(sign_count, random_state=0).fit(X)
            centroids = kmeans_output.cluster_centers_.tolist()
            # del(centroids[1])

            #Now need to find the closest node to each centroid
            signal_placements = []
            possible_nodes_temp = repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].find()[0]['nodes']

            #Convert to dictionary
            possible_nodes = dict()
            for i in range(len(possible_nodes_temp)):
                possible_nodes[i] = possible_nodes_temp[i]

            #find nodes closest to each centroid
            taken = []
            failed_count = 0
            for i in range(len(centroids)):                     #calculated centroids loop
                point = centroids[i]
                closest_node = None
                dist_of_closest_node = float('inf')

                for j in range(len(possible_nodes)):            #possible intersections
                    if j not in taken:

                        # j also cannot be within .5 miles of anything in taken...
                        skip_this = False
                        for placed in signal_placements:        #already placed signals

                            j_to_placed = vincenty(placed, possible_nodes[j]).miles
                            if j_to_placed <= buffer_size:
                                skip_this = True
                                break
                        if skip_this:
                            continue
                        distance = vincenty(centroids[i], possible_nodes[j]).miles
                        if distance < dist_of_closest_node:
                            closest_node = j
                            dist_of_closest_node = distance

                try:
                    taken.append(closest_node)
                    signal_placements.append(possible_nodes[closest_node])
                except KeyError:
                    failed_count += 1
                    if logging:
                        print("A suitable intersection was not found.\nCurrently, there are",failed_count,\
                        "centroids that were not able to be placed on intersections.")



            signal_placement_dict = {'signal_placements': signal_placements}
            repo['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements'].insert_one(signal_placement_dict)
            repo['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements'].metadata({'complete':True})

            repo.logout()
            endTime = datetime.datetime.now()
            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/adsouza_bmroach_mcaloonj_mcsmocha')
            doc.add_namespace('dat', 'http://datamechanics.io/data/adsouza_bmroach_mcaloonj_mcsmocha')
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

            #Agent
            this_script = doc.agent('alg:get_signal_placements', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

            #Resources
            clean_triggers = doc.entity('dat:clean_triggers', {'prov:label': 'Clean Triggers', prov.model.PROV_TYPE:'ont:DataResource'})
            nodes = doc.entity('dat:nodes', {'prov:nodes': 'Nodes', prov.model.PROV_TYPE:'ont:DataResource'})

            #Activities
            this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

            #Usage
            doc.wasAssociatedWith(this_run, this_script)

            doc.used(this_run, clean_triggers, startTime)
            doc.used(this_run, nodes, startTime)

            signal_placements = doc.entity('dat:signal_placements', {prov.model.PROV_LABEL:'Signal Placements',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(signal_placements, this_script)
            doc.wasGeneratedBy(signal_placements, this_run, endTime)
            doc.wasDerivedFrom(signal_placements, clean_triggers, this_run, this_run, this_run)
            doc.wasDerivedFrom(signal_placements, nodes, this_run, this_run, this_run)

            repo.logout()
            return doc

# get_signal_placements.execute()
# doc = get_signal_placements.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
