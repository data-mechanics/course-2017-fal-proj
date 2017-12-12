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

class kmeans_iterative(dml.Algorithm):
    contributor = 'esaracin'
    reads = ['esaracin.crime_incidents']
    writes = ['esaracin.crime_incident_centers_iterative']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        dataset = repo['esaracin.crime_incidents'].find()
        df_crime = pd.DataFrame(list(dataset))
        if(trial == True):
            df_crime = df_crime.sample(frac=.05)

        
        df_coordinates = {'Latitude': [], 'Longitude': []}
        for index, row in df_crime.iterrows():
            lat_long = eval(row['Location'])
            
            # There are many entries without Lat/Long data; remove them so as
            # to avoid affecting the clustering.
            if(lat_long == (0, 0) or lat_long == (-1, -1)):
                continue

            df_coordinates['Latitude'] += [lat_long[0]]
            df_coordinates['Longitude'] += [lat_long[1]]

        df_coordinates = pd.DataFrame(df_coordinates)


        center_dict = {key:[0] * 20 for key in range(1, 21)}
        for i in range(1, 2):
            kmeans = KMeans(init='k-means++', n_clusters=i)
            kmeans.fit_predict(df_coordinates)
            centers = kmeans.cluster_centers_
    
            for center in range(len(centers)):
                center_dict[i][center] = (list(centers[center]))


        new_df = pd.DataFrame(center_dict)

        r = new_df.to_json(orient='records')
        # Now, we can put these centers into a new MongoDB collection
        r = json.loads(r)
        repo.dropCollection("crime_incident_centers_iterative")
        repo.createCollection("crime_incident_centers_iterative")
        repo['esaracin.crime_incident_centers_iterative'].insert_many(r)
        repo['esaracin.crime_incident_centers_iterative'].metadata({'complete':True})
        print(repo['esaracin.crime_incident_centers_iterative'].metadata())


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
        doc.usage(clustering, resource_crimes, startTime, None, {prov.model.PROV_TYPE:'ont:Transformation'})

        clustered = doc.entity('dat:esaracin#crime_incident_centers_iterative', {prov.model.PROV_LABEL:'Clustered Set', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clustered, this_script)
        doc.wasGeneratedBy(clustered, clustering, endTime)
        doc.wasDerivedFrom(clustered, resource_crimes, clustering, clustering, clustering)


        repo.logout()
        return doc

