"""
Filename: make_graph.py

Last edited by: BMR 11/12/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Development Notes: 
- Have to run execute.py in order to get placements.html file. Shouln't run make_graph file separately.

"""

import geojson
from geoql import geoql
import geoleaflet
import dml
import pandas as pd, requests, json
import datetime
import prov.model
import uuid


class make_graph(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = ['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements']
        writes = []

        @staticmethod
        def execute(trial=False, logging=True):
            startTime = datetime.datetime.now()

            if logging:
                print("in make_graph.py")

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')


            points = repo['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements'].find()
            coords = points[0]['signal_placements']
            df_geo = pd.DataFrame(coords, columns=['Lats', "Longs"])

            # Turn a dataframe containing point data into a geojson formatted python dictionary	
            def df_to_geojson(df, properties, lat='Lats', lon='Longs'):
                geo_json = {'type':'FeatureCollection', 'features':[]}
                for _, row in df.iterrows():
                    feature = {'type':'Feature',
                            'properties':{},
                            'geometry':{'type':'Point',
                                        'coordinates':[]}}
                    feature['geometry']['coordinates'] = [row[lon],row[lat]]
                    for prop in properties:
                        feature['properties'][prop] = row[prop]
                    geo_json['features'].append(feature)
                return geo_json

            placements_geojson = df_to_geojson(df_geo, properties="")
            with open('./adsouza_bmroach_mcaloonj_mcsmocha/placements.html', 'w') as output:
                output.write(geoleaflet.html(placements_geojson)) # Create visualization.


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
            doc.add_namespace('dbg','https://data.boston.gov/api/3/action/datastore_search')

            this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#make_graph', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
            resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'make_graph', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

            make_graph = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(make_graph, this_script)

            doc.usage(make_graph, resource, startTime, None,
                      {prov.model.PROV_TYPE:'ont:Retrieval',
                      'ont:Query':'?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000'
                      }
                      )

            graph = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#graph', {prov.model.PROV_LABEL:'Graph',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(graph, this_script)
            doc.wasGeneratedBy(graph, make_graph, endTime)
            doc.wasDerivedFrom(graph, resource, make_graph, make_graph, make_graph)

            repo.logout()
            return doc

# make_graph.execute()