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

#Extract street data
class fetch_nodes(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = ['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters']
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.nodes']

        @staticmethod
        def execute(trial=False):
            startTime = datetime.datetime.now()
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
            g = geoql.loads(requests.get('http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson').text, encoding="latin-1")

            g = g.keep_within_radius((42.3551, -71.0656), 2, 'miles')

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

            # clusters =  [[ 42.35657779, -71.05989936],
            # [ 42.31588585, -71.1020072 ],
            # [ 42.30923647, -71.06225799],
            # [ 42.25518682, -71.12487349],
            # [ 42.35224748, -71.13538298],
            # [ 42.34558858, -71.07971195],
            # [ 42.38695412, -71.01158449],
            # [ 42.28634076, -71.12310923],
            # [ 42.28683856, -71.06414815],
            # [ 42.27939484, -71.15648171],
            # [ 42.33405656, -71.04954215],
            # [ 42.37946809, -71.06687642],
            # [ 42.33770076, -71.10208356],
            # [ 42.30389511, -71.08092185],
            # [ 42.32745237, -71.07741856],
            # [ 42.28320965, -71.08846939],
            # [ 42.37609957, -71.03556853],
            # [ 42.3486924 , -71.15705574]]


            #Insert clusters into cKDTree
            cluster_tree = cKDTree(clusters)

            distances = []
            #For each node, get distance of nearest cluster
            for i in range(len(nodes)):
                #find the k nearest neighbors
                dist, idx = cluster_tree.query(nodes[i], k=1, p=2) #p=2 means euclidean distance, there's no option for vincenty
                distances.append(dist)

            #Get average distance to closest cluster
            mean = sum(distances)/len(distances)

            #Filter out nodes that have distance to nearest cluster that is less than the mean
            filtered_nodes = []
            for i in range(len(nodes)):
                dist, idx = cluster_tree.query(nodes[i], k=1, p=2)
                if dist >= mean:
                    filtered_nodes.append(nodes[i])


            #Insert into repo {"nodes": [[lat,long], [lat,long]..............]}
            tmp = dict()
            tmp['nodes'] = filtered_nodes
            filtered_nodes = tmp


            repo.dropCollection("nodes")
            repo.createCollection("nodes")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].insert(filtered_nodes)
            repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].metadata({'complete':True})
            #print(repo['adsouza_bmroach_mcaloonj_mcsmocha.hospitals'].metadata())

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

'''
fetch_nodes.execute()
doc = fetch_nodes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
