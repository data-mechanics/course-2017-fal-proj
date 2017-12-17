"""
Filename: logic.py

Last edited by: BMR 12/2/17

Boston University CS591 Data Mechanics Fall 2017 - Project 3
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Original file provided by Andrei Lapets (lapets@bu.edu)
Web: datamechanics.org
Version 0.0.3

Was originally execute.py from parent directory. Modified for use by
web service for project 3

Development Notes:
-trialRun not currently operable


"""
import sys
import os
import importlib
import json
import argparse
import prov.model
import protoql
import datetime


def algo(parameters, requestCount, threadID, trialRun=False, doProv=False):

    # Extract the algorithm classes from the modules in the
    # current directory.
    startTime = datetime.datetime.now()
    path = "."
    excluded = ["logic.py", "server.py", "our_geoleaflet.py"]
    algorithms = []
    for r,d,f in os.walk(path):
        for file in f:            
            if r.find(os.sep) == -1 and file.split(".")[-1] == "py" and file not in excluded:
                name_module = ".".join(file.split(".")[0:-1])
                module = importlib.import_module( name_module)
                algorithms.append(module.__dict__[name_module])

    # Create an ordering of the algorithms based on the data
    # sets that they read and write.
    datasets = set()
    ordered = []
    while len(algorithms) > 0:
        for i in range(0,len(algorithms)):
            if set(algorithms[i].reads).issubset(datasets):
                datasets = datasets | set(algorithms[i].writes)
                ordered.append(algorithms[i])
                del algorithms[i]
                break
                

    # Execute the algorithms in order.
    if doProv:
        provenance = prov.model.ProvDocument()

    for algorithm in ordered:
        algo_name = str(algorithm)

        #skip algorithms which fetch and have already been called, but check again every 10 requests
        if 'fetch' in algo_name and \
        'node' not in algo_name and \
        requestCount > 2 and \
        requestCount % 10 != 0:
            continue
            
        completed = False
        while not completed:
            try:                
                if 'fetch_nodes' in algo_name:
                    ms = parameters['mean_skew']
                    r = parameters['radius']
                    algorithm.execute(trial=trialRun,mean_skew=ms, radius=r)

                elif 'get_accident_clusters' in algo_name:
                    cd = parameters['cluster_divisor']
                    algorithm.execute(trial=trialRun,cluster_divisor=cd)
                
                elif 'get_signal_placements' in algo_name:
                    sc = parameters['sign_count']
                    bs = parameters['buffer_size']
                    algorithm.execute(trial=trialRun,sign_count=sc, buffer_size=bs)

                elif 'get_avg_distance' in algo_name:
                    algorithm.execute(trial=trialRun, web=True)
                    
                elif 'make_graph' in algo_name:
                    algorithm.execute(trial=trialRun, threadID=threadID)

                else:
                    algorithm.execute(trial=trialRun)

                if doProv:
                    provenance = algorithm.provenance(provenance)
                
                completed = True
                
            except:
                print("There was an error in", algo_name, "\nAttempting again...")


    if doProv:
        # Display a provenance record of the overall execution process.
        print(provenance.get_provn())

        # Render the provenance document as an interactive graph.
        prov_json = json.loads(provenance.serialize())

        agents = [[a] for a in prov_json['agent']]
        entities = [[e] for e in prov_json['entity']]
        activities = [[v] for v in prov_json['activity']]
        wasAssociatedWith = [(v['prov:activity'], v['prov:agent'], 'wasAssociatedWith') for v in prov_json['wasAssociatedWith'].values()]
        wasAttributedTo = [(v['prov:entity'], v['prov:agent'], 'wasAttributedTo') for v in prov_json['wasAttributedTo'].values()]
        wasDerivedFrom = [(v['prov:usedEntity'], v['prov:generatedEntity'], 'wasDerivedFrom') for v in prov_json['wasDerivedFrom'].values()]
        wasGeneratedBy = [(v['prov:entity'], v['prov:activity'], 'wasGeneratedBy') for v in prov_json['wasGeneratedBy'].values()]
        used = [(v['prov:activity'], v['prov:entity'], 'used') for v in prov_json['used'].values()]
        open('provenance.html', 'w').write(protoql.html("graph(" + str(entities + agents + activities) + ", " + str(wasAssociatedWith + wasAttributedTo + wasDerivedFrom + wasGeneratedBy + used) + ")"))

    endTime = datetime.datetime.now()
    elapsed = endTime - startTime
    print("Elapsed time to run algorithm:", elapsed)
    return

if __name__ == '__main__':
    params = {'mean_skew': 1.0, #fetch_nodes
              'radius': 2, #fetch_nodes
              'cluster_divisor': 15, #get_accident_clusters
              'sign_count': 30, #get_signal_placements 
              'buffer_size': .5, #get_signal_placements 
             }
    algo(params, 1)