"""
Filename: get_avg_distance.py

Last edited by: AD 11/14/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Original skeleton files provided by Andrei Lapets (lapets@bu.edu)

Development Notes:
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import matplotlib.pyplot as plt

class get_avg_distance(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'

    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.avg_distance']

    @staticmethod
    def execute(trial=False, logging=True, web=False):
        startTime = datetime.datetime.now()

        if logging:
            print("in get_avg_distance.py")

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
        repo.dropCollection("avg_distance")
        repo.createCollection("avg_distance")

        cluster_stats = repo['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats'].find({}, {'_id': False})
        cluster_stats = [d for d in cluster_stats]

        # turn data into lists containing each of the data points
        cluster_list = []
        avg_school_dist = []
        avg_hosp_dist = []
        avg_park_dist = []
        avg_acc_dist = []

        for e in cluster_stats:
            for k in e:
                cluster_list.append(int(k)) # add cluster number
                avg_school_dist.append(e[k][0]) # add avg distance from school
                avg_hosp_dist.append(e[k][1]) # add avg distance from hospital
                avg_park_dist.append(e[k][2]) # add avg distance from park
                avg_acc_dist.append(e[k][3]) # add avg distance from accident point

        # create list of overall averages as
        average_dist = []
        for i in range(len(cluster_list)):
            average_dist.append(np.mean([avg_school_dist[i], avg_hosp_dist[i], avg_park_dist[i], avg_acc_dist[i]]))

        # print lowest average and the k value associated with it
        best_k_ind = 0
        lowest_avg = average_dist[0]
        for i in range(len(cluster_list)):
            if lowest_avg > average_dist[i]:
                best_k_ind = i
                lowest_avg = average_dist[i]

        best_k = cluster_list[best_k_ind]

        # graph all the averages on the same graph
        if not web:
            plt.plot(cluster_list, avg_school_dist, 'b', label='Schools')
            plt.plot(cluster_list, avg_hosp_dist, 'r', label='Hospitals')
            plt.plot(cluster_list, avg_park_dist, 'g', label='Parks')
            plt.plot(cluster_list, avg_acc_dist, 'm', label='Accidents')
            plt.plot(cluster_list, average_dist, 'k', linewidth=3, label='Overall Average')
            plt.title('Average Distances from Triggers')
            plt.xlabel('Number of Clusters')
            plt.ylabel('Average Distance in Miles')
            plt.legend()
            plt.show()

        # create list of dictionaries for output
        output_ldict = []
        for i in range(len(cluster_list)):
            clust_dict = {}
            clust_dict['cluster_num'] = cluster_list[i]
            clust_dict['overall_average'] = average_dist[i]
            output_ldict.append(clust_dict)

        repo['adsouza_bmroach_mcaloonj_mcsmocha.avg_distance'].insert_many(output_ldict)
        repo['adsouza_bmroach_mcaloonj_mcsmocha.avg_distance'].metadata({'complete':True})

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/adsouza_bmroach_mcaloonj_mcsmocha/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/adsouza_bmroach_mcaloonj_mcsmocha/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

        #Agent
        this_script = doc.agent('alg:get_avg_distance', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

        #Resources
        resource = doc.entity('dat:num_clusters_stats', {'prov:label': 'Num Clusters Stats', prov.model.PROV_TYPE:'ont:DataResource'})

        #Activities
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        #Usage
        doc.wasAssociatedWith(this_run, this_script)

        doc.used(this_run, resource, startTime)

        #New dataset
        avg_distance = doc.entity('dat:avg_distance', {prov.model.PROV_LABEL:'Average Distances',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(avg_distance, this_script)
        doc.wasGeneratedBy(avg_distance, this_run, endTime)
        doc.wasDerivedFrom(avg_distance, resource, this_run, this_run, this_run)

        repo.logout()
        return doc

# get_avg_distance.execute()
# doc = get_signal_placements.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
