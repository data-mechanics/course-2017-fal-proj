import geojson
from geoql import geoql
import geoleaflet
import requests
import dml
import prov.model
import datetime
import uuid
import json

#Extract street data
class fetch_nodes(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = []
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
            point_dict = [obj for obj in j if (obj['type'] == "Point")]
            open('leaflet.html', 'w').write(geoleaflet.html(g)) # Create visualization.

            repo.dropCollection("nodes")
            repo.createCollection("nodes")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.nodes'].insert_many(point_dict)
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

            this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#fetch_nodes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            resource = doc.entity('bod:cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson', {'prov:label':'Boston Segments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

            get_nodes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_nodes, this_script)

            doc.usage(get_nodes, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'})

            nodes = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#nodes', {prov.model.PROV_LABEL:'Nodes', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(nodes, this_script)
            doc.wasGeneratedBy(nodes, get_nodes, endTime)
            doc.wasDerivedFrom(nodes, resource, get_nodes, get_nodes, get_nodes)

            repo.logout()

            return doc


fetch_nodes.execute()
doc = fetch_nodes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
