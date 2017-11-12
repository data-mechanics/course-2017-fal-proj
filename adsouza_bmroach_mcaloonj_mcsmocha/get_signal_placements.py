"""
Filename: get_signal_placements.py

Last edited by: BMR 11/11/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Original skeleton files provided by Andrei Lapets (lapets@bu.edu)

Development Notes: 
-

"""

import sklearn
from sklearn.preprocessing import Normalizer, StandardScaler, MinMaxScaler
from sklearn import metrics
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
        def execute(trial=False):
            startTime = datetime.datetime.now()
            
            if trial:
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

            n_clusters = 30
            X =  np.array(triggers)

            kmeans_output = KMeans(n_clusters, random_state=0).fit(X)
            centroids = kmeans_output.cluster_centers_.tolist()
            del(centroids[1])
 
            #Now need to find the closest node to each centroid
            signal_placements = []
            possible_nodes_temp = repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].find()[0]['nodes']
            
            #Convert to dictionary
            possible_nodes = dict()
            for i in range(len(possible_nodes_temp)):
                possible_nodes[i] = possible_nodes_temp[i]
            
            #find nodes closest to each centroid
            taken = []
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
                            if j_to_placed < .5:
                                skip_this = True
                                break
                        if skip_this:
                            continue
                        distance = vincenty(centroids[i], possible_nodes[j]).miles
                        if distance < dist_of_closest_node:
                            closest_node = j
                            dist_of_closest_node = distance
                
                taken.append(closest_node)
                signal_placements.append(possible_nodes[closest_node])

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

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
            doc.add_namespace('dbg','https://data.boston.gov')

            this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#get_signal_placements', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
            resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Signal Placements', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

            get_signal_placements = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_signal_placements, this_script)

            doc.usage(get_signal_placements, resource, startTime, None,
                      {prov.model.PROV_TYPE:'ont:Retrieval',
                      'ont:Query':'6222085d-ee88-45c6-ae40-0c7464620d64'
                      }
                      )

            signal_placements = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#signal_placements', {prov.model.PROV_LABEL:'Signal Placements',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(signal_placements, this_script)
            doc.wasGeneratedBy(signal_placements, get_signal_placements, endTime)
            doc.wasDerivedFrom(signal_placements, resource, get_signal_placements, get_signal_placements, get_signal_placements)

            repo.logout()
            return doc

# get_signal_placements.execute()
# doc = get_signal_placements.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
