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
import our_geoleaflet
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
        def execute(trial=False, logging=True, threadID=0):
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
            filename = "./templates/placements"+str(threadID)+".html"
            with open(filename, 'w') as output:
                output.write(our_geoleaflet.html(placements_geojson)) # Create visualization.


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
            this_script = doc.agent('alg:make_graph', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

            #Resources
            resource = doc.entity('dat:signal_placements', {'prov:label': 'Signal Placements', prov.model.PROV_TYPE:'ont:DataResource'})

            #Activities
            this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

            #Usage
            doc.wasAssociatedWith(this_run, this_script)

            doc.used(this_run, resource, startTime)

            #New dataset
            graph = doc.entity('dat:graph', {prov.model.PROV_LABEL:'Graph',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(graph, this_script)
            doc.wasGeneratedBy(graph, this_run, endTime)
            doc.wasDerivedFrom(graph, resource, this_run, this_run, this_run)

            repo.logout()
            return doc

# make_graph.execute()
