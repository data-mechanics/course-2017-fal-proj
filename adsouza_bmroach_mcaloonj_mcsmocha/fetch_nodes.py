"""
Filename: fetch_nodes.py

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

import geojson
from geoql import geoql
import geoleaflet
import requests
import dml
import prov.model
import datetime
import uuid
import json
from scipy.spatial import cKDTree

class fetch_nodes(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = ['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters']
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.nodes']

        @staticmethod
        def execute(trial=False, logging=True):
            startTime = datetime.datetime.now()

            #__________________________
            #Parameters
            mean_skew = 1.0
            # ^ allows mean (for checking bottom 50th percent of distances) to be skewed, to allow in more or less.
            # decreasing value decreases the mean, so fewer are allowed in

            assert type(mean_skew) == float and mean_skew > 0

            if trial:
                keep_within_value = .5
            else:
                keep_within_value = 2

            #End Parameters
            #__________________________

            if logging:
                print("in fetch_nodes.py")

            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
            g = geoql.loads(requests.get('http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson').text, encoding="latin-1")

            g = g.keep_within_radius((42.3551, -71.0656), keep_within_value, 'miles')

            g = g.node_edge_graph()

            g.dump(open('example_extract.geojson', 'w'))

            f = open('example_extract.geojson', 'r')
            f = f.read()
            j = json.loads(f)["features"]


            #Creates dictionary with only nodes of the graph
            nodes= [[obj['coordinates'][1],obj['coordinates'][0]] for obj in j if (obj['type'] == "Point")]
            #print (len(nodes))
            open('leaflet.html', 'w').write(geoleaflet.html(g)) # Create visualization.

            clusters = repo['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters'].find_one()
            clusters = clusters["accident_clusters"]

            #Insert clusters into cKDTree
            cluster_tree = cKDTree(clusters)

            distances = []
            #For each node, get distance of nearest cluster
            for i in range(len(nodes)):
                #find the k nearest neighbors
                dist, idx = cluster_tree.query(nodes[i], k=1, p=2) #p=2 means euclidean distance, there's no option for vincenty
                distances.append((dist,idx))

            #Get average distance to closest cluster
<<<<<<< HEAD
            mean = sum([x[0] for x in distances])/len(distances)
=======
            mean = ( sum(distances)/len(distances) ) * mean_skew
>>>>>>> 4a57574a08b1ee2fd43c8fc14a978a218456b375

            #Filter out nodes that have distance to nearest cluster that is less than the mean
            filtered_nodes = [nodes[x[1]] for x in distances if x[0] >= mean]


            #Insert into repo {"nodes": [[lat,long], [lat,long]..............]}
            tmp = dict()
            tmp['nodes'] = filtered_nodes
            filtered_nodes = tmp


            repo.dropCollection("nodes")
            repo.createCollection("nodes")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].insert(filtered_nodes)
            repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].metadata({'complete':True})

            repo.logout()
            endTime = datetime.datetime.now()
            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo

            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
            doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson')
            doc.add_namespace('abmm', 'adsouza_bmroach_mcaloonj_mcsmocha')

            this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_nodes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            streets = doc.entity('bod:cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson', {'prov:label':'Boston Segments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            accident_clusters = doc.entity('abmm:accident_clusters', {'prov:label':'Accident Clusters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

            get_nodes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_nodes, this_script)

            doc.usage(get_nodes, streets, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'})

            doc.usage(get_nodes, accident_clusters, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'})


            nodes = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#nodes', {prov.model.PROV_LABEL:'Nodes',prov.model.PROV_TYPE:'ont:DataSet'})


            doc.wasAttributedTo(nodes, this_script)
            doc.wasGeneratedBy(nodes, get_nodes, endTime)
            doc.wasDerivedFrom(nodes, streets, get_nodes, get_nodes, get_nodes)
            doc.wasDerivedFrom(nodes, accident_clusters, get_nodes, get_nodes, get_nodes)

            repo.logout()

            return doc


#fetch_nodes.execute()
# doc = fetch_nodes.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


#eof
