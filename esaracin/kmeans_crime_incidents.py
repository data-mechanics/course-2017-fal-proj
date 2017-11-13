import urllib.request
import json
import pandas as pd
import numpy as np
import dml
import prov.model
import datetime
import uuid
import sys
from bson import json_util
from sklearn.cluster import KMeans
import matplotlib as mp
mp.use('Agg')
import matplotlib.pyplot as plt

class kmeans_crime_incidents(dml.Algorithm):
    contributor = 'esaracin'
    reads = ['esaracin.crime_incidents']
    writes = ['esaracin.crime_incident_centers']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Grab all of our two databases that we're reading from.
        # Note that, if trial is set to True, we only take a random, 5% sample
        # of our overall crime_incidents, to speed things up.
        dataset = repo['esaracin.crime_incidents'].find()
        df_crime = pd.DataFrame(list(dataset))
        if(trial == True):
            df_crime = df_crime.sample(frac=.05)



        # Extract the lat/long tuples.
        location = df_crime['Location']

        df_coordinates = {key:[] for key in ['Latitude', 'Longitude']}

        for index, row in df_crime.iterrows():
            lat_long = eval(row['Location'])
            
            # There are many entries without Lat/Long data; remove them so as
            # to avoid affecting the clustering.
            if(lat_long == (0, 0) or lat_long == (-1, -1)):
                continue

            df_coordinates['Latitude'] += [lat_long[0]]
            df_coordinates['Longitude'] += [lat_long[1]]

        df_coordinates = pd.DataFrame(df_coordinates)

        # Now, we can run kmeans++ on our set of coordinates to find the best
        # cluster centers.
        kmeans = KMeans(init='k-means++', n_clusters=5)
        kmeans.fit_predict(df_coordinates)
        clusters, centers = kmeans.labels_, kmeans.cluster_centers_


        new_df = {key:[] for key in ['Latitude', 'Longitude']}
        for center in centers:
            new_df['Latitude'] += [center[0]]
            new_df['Longitude'] += [center[1]]

        new_df = pd.DataFrame(new_df)

        # Now, we can put these centers into a new MongoDB collection
        json_set = new_df.to_json(orient='records')
        r = json.loads(json_set)

        repo.dropCollection("crime_incident_centers")
        repo.createCollection("crime_incident_centers")
        repo['esaracin.crime_incident_centers'].insert_many(r)
        repo['esaracin.crime_incident_centers'].metadata({'complete':True})
        print(repo['esaracin.crime_incident_centers'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        '''Creates the provenance document describing the merging of data
        occuring within this script.'''
         
        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Add useful namespaces for this prov doc
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics/io/ontology/')
        doc.add_namespace('log', 'http://datamechanics.io/log/')

        # Add this script as a provenance agent to our document. Also add the
        # entity and activity utilized and completed by this script.
        this_script = doc.agent('alg:esaracin#kmeans_crime_incidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crimes = doc.entity('dat:esaracin#crime_incidents',{'prov:label':'MongoDB Set',prov.model.PROV_TYPE:'ont:DataResource'})
        clustering = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(clustering, this_script)
        doc.usage(clustering, resource_crimes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        clustered = doc.entity('dat:esaracin#crime_incident_centers', {prov.model.PROV_LABEL:'Clustered Set', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clustered, this_script)
        doc.wasGeneratedBy(clustered, clustering, endTime)
        doc.wasDerivedFrom(clustered, resource_crimes, clustering, clustering, clustering)


        repo.logout()
        return doc

