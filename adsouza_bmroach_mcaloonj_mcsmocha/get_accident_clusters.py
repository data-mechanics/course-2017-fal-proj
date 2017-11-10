import sklearn
# Import all of the scikit learn stuff
from sklearn.preprocessing import Normalizer, StandardScaler, MinMaxScaler
from sklearn import metrics 
from sklearn.cluster import KMeans
import numpy as np
import json
from scipy import stats
import datetime
import itertools
import collections
import dml
import prov.model
import urllib.request
import uuid

# This file will cluster accident hotspots using K-Means

class get_accident_clusters(dml.Algorithm):
        contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
        reads = ['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers']
        writes = ['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters']

        @staticmethod
        def execute(trial=False):
            startTime = datetime.datetime.now()
            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

            
            # response = requests.get(url)
            # r = response.json()
            # s = json.dumps(r, sort_keys=True, indent=2)
            # #print (s)
            # #print ()

            for i in range(10):
                print('touching Adrianas stuff @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            
            da_accidents = repo['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers'].find()
            coords_input = da_accidents['accidents']
            
            # coords_input = a dictionary of a list of tuples of lats and longs

            n_clusters = 4
            X =  np.array(coords_input)
            # looks like [(lat, long), (lat, long), (lat, long)...]
            
            kmeans = KMeans(n_clusters, random_state=0).fit(X)
            centroids = kmeans.cluster_centers_

            repo.dropCollection("accident_clusters")
            repo.createCollection("accident_clusters")

            repo['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters'].insert_many(centroids)
            repo['adsouza_bmroach_mcaloonj_mcsmocha.accident_clusters'].metadata({'complete':True})

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

            this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#get_accident_clusters', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
            resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Accident Centroids', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

            get_accident_hotspots = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_accident_hotspots, this_script)

            doc.usage(get_hospitals, resource, startTime, None,
                      {prov.model.PROV_TYPE:'ont:Retrieval',
                      'ont:Query':'6222085d-ee88-45c6-ae40-0c7464620d64'
                      }
                      )

            accident_clusters = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#accident_clusters', {prov.model.PROV_LABEL:'Accident Clusters',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(accident_clusters, this_script)
            doc.wasGeneratedBy(accident_clusters, get_accident_hotspots, endTime)
            doc.wasDerivedFrom(accident_clusters, resource, get_accident_hotspots, get_accident_hotspots, get_accident_hotspots)

            repo.logout()
            return doc
'''
get_accident_clusters.execute()
doc = get_accident_clusters.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
