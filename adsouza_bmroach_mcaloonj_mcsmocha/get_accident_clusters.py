"""
Filename: get_accident_clusters.py

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

class get_accident_clusters(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = ['adsouza_bmroach_mcaloonj_mcsmocha.accidents']
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters']

        @staticmethod
        def execute(trial=False, logging=True, cluster_divisor=15):
            """
            Parameters:
            divide accident count by cluster_divisor, and there's that many clusters
            Ex 200 accidents divided by cluster_divisor of 10 is 20 clusters, or means
            """

            startTime = datetime.datetime.now()

            if logging:
                print("in get_accident_clusters.py")

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')


            accidents = repo['adsouza_bmroach_mcaloonj_mcsmocha.accidents'].find()
            coords_input = [tuple(row['Location'].replace('(', '').replace(')', '').split(','))
                            for row in accidents if row['Location'] != '(0.00000000, 0.00000000)' ]

            coords_input = [(float(lat), float(lon)) for (lat, lon) in coords_input]

            n_clusters = len(coords_input)//cluster_divisor
            X =  np.array(coords_input)
            # looks like [(lat, long), (lat, long), (lat, long)...]

            kmeans_output = KMeans(n_clusters, random_state=0).fit(X)
            centroids = kmeans_output.cluster_centers_.tolist()
            del(centroids[1])
            accident_clusters_dict = {'accident_clusters': centroids}

            repo.dropCollection("accident_clusters")
            repo.createCollection("accident_clusters")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters'].insert_one(accident_clusters_dict)
            repo['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters'].metadata({'complete':True})

            repo.logout()

            endTime = datetime.datetime.now()

            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/adsouza_bmroach_mcaloonj_mcsmocha/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/adsouza_bmroach_mcaloonj_mcsmocha/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

            #Agent
            this_script = doc.agent('alg:get_accident_clusters', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

            #Resource
            resource = doc.entity('dat:accidents', {'prov:label': 'Accidents', prov.model.PROV_TYPE:'ont:DataResource'})

            #Activities
            this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

            #Usage
            doc.wasAssociatedWith(this_run, this_script)

            doc.used(this_run, resource, startTime)

            #New dataset
            accident_clusters = doc.entity('dat:accident_clusters', {prov.model.PROV_LABEL:'Accident Clusters',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(accident_clusters, this_script)
            doc.wasGeneratedBy(accident_clusters, this_run, endTime)
            doc.wasDerivedFrom(accident_clusters, resource, this_run, this_run, this_run)

            repo.logout()
            return doc

# get_accident_clusters.execute()
# doc = get_accident_clusters.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
