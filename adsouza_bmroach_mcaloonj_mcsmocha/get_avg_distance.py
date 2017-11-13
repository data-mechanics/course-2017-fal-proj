"""
Filename: get_avg_correlation.py

Last edited by: BMR 11/12/17

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
    #reads = ['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements', 'adsouza_bmroach_mcaloonj_mcsmocha.street_info']
    #writes = ['adsouza_bmroach_mcaloonj_mcsmocha.speed_stats']
    
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.avg_distance']

    @staticmethod
    def execute(trial=False, logging=True):
        startTime = datetime.datetime.now()
        
        if logging:
            print("in get_avg_distance.py")
            
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
        repo.dropCollection("avg_distance")
        repo.createCollection("avg_distance")

        cluster_stats = repo['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats'].find()
        cluster_stats = [d for d in cluster_stats]
        print(cluster_stats)
        # turn data into lists containing each of the data points
        cluster_list = []
        avg_school_dist = []
        avg_hosp_dist = []
        avg_park_dist = []
        avg_acc_dist = []

        for c in cluster_stats:
            cluster_list.append(int(c)) # add cluster number
            avg_school_dist.append(cluster_stats[c][0]) # add avg distance from school
            avg_hosp_dist.append(cluster_stats[c][1]) # add avg distance from hospital
            avg_park_dist.append(cluster_stats[c][2]) # add avg distance from park
            avg_acc_dist.append(cluster_stats[c][3]) # add avg distance from accident point

        # create list of overall averages as 
        average_dist = []
        for i in range(len(cluster_list)):
            average_dist.append(np.mean([avg_school_dist[i], avg_hosp_dist[i], avg_park_dist[i], avg_acc_dist[i]]))

        print(average_dist)
        # print lowest average and the k value associated with it
        best_k_ind = 0
        lowest_avg = average_dist[0]
        for i in range(len(cluster_list)):
            if lowest_avg > average_dist[i]:
                best_k_ind = i
                lowest_avg = average_dist[i]

        best_k = cluster_list[best_k_ind]
        print(best_k)

        # graph all the averages on the same graph
        plt.plot(cluster_list, avg_school_dist, 'b', label='Schools')
        plt.plot(cluster_list, avg_hosp_dist, 'r', label='Hospitals')
        plt.plot(cluster_list, avg_park_dist, 'g', label='Parks')
        plt.plot(cluster_list, avg_acc_dist, 'm', label='Accidents')
        plt.plot(cluster_list, average_dist, 'k', linewidth=2.5, label='Overall Average')
        plt.title('Average Distances from Sign for Different K Clusters')
        plt.xlabel('Number of Clusters')
        plt.ylabel('Average Distance')
        plt.legend()
        plt.show()

        # create list of dictionaries for output
        output_ldict = []
        for i in range(len(cluster_list)):
            clust_dict = {}
            clust_dict['cluster_num'] = cluster_list[i]
            clust_dict['overall_average'] = average_dist[i]
        
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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dbg','https://data.boston.gov')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#get_avg_correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Speed Stats', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

        get_avg_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_avg_correlation, this_script)

        doc.usage(get_avg_correlation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'6222085d-ee88-45c6-ae40-0c7464620d64'
                  }
                  )

        speed_stats = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#speed_stats', {prov.model.PROV_LABEL:'Speed Stats',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(speed_stats, this_script)
        doc.wasGeneratedBy(speed_stats, get_avg_correlation, endTime)
        doc.wasDerivedFrom(speed_stats, resource, get_avg_correlation, get_avg_correlation, get_avg_correlation)

        repo.logout()
        return doc
            
# get_avg_correlation.execute()
# doc = get_signal_placements.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))