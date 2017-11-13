import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class boston_hubway_stations(dml.Algorithm):
    contributor = 'jtbloom_rfballes_medinad'
    reads = ['jtbloom_rfballes_medinad.hubway_stations']
    writes = ['jtbloom_rfballes_medinad.boston_hubway_stations']


    def project(x,y):
        return{y(t) for t in x}

    def aggregate(R,f):
        keys = {r[0] for r in R}
        return {(key, f([v for (k,v) in R if k == key])) for key in keys}

    @staticmethod
    def execute(trial = False):
        pass
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

        repo.dropCollection("jtbloom_rfballes_medinad.boston_hubway_stations")
        repo.createCollection("jtbloom_rfballes_medinad.boston_hubway_stations")

        hubway_list = []
        
        for item in repo.jtbloom_rfballes_medinad.hubway_stations.find():
            new_dict = {}
            new_dict['Station Name'] = item['Station']
            new_dict['Longitude'] = item['Longitude']
            new_dict['Latitude'] = item['Latitude']
            #new_dict['Municipality'] = item['Municipality']
            new_dict['Number of Docks'] = item['# of Docks']    
            hubway_list.append(new_dict)
        #print(hubway_list)

        x = boston_hubway_stations.project(hubway_list, lambda t: (t['Station Name'], t['Number of Docks'], t['Latitude'], t['Longitude']))
        x = [{'Station': s, 'Number of Docks': n, 'Latitude': lat, 'Longitude': lon} for (s, n, lat, lon) in x]


        repo['jtbloom_rfballes_medinad.boston_hubway_stations'].insert_many(x)


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

boston_hubway_stations.execute()