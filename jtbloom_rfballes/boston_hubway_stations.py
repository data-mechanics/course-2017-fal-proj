import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class boston_hubway_stations(dml.Algorithm):
    contributor = 'jtbloom_rfballes'
    reads = ['jtbloom_rfballes.hubway_stations']
    writes = ['jtbloom_rfballes.boston_hubway_stations']


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
        repo.authenticate('jtbloom_rfballes', 'jtbloom_rfballes')

        repo.dropCollection("jtbloom_rfballes.boston_hubway_stations")
        repo.createCollection("jtbloom_rfballes.boston_hubway_stations")

        hubway_list = []

        for item in repo.jtbloom_rfballes.hubway_stations.find():
           for feature in item["features"]:
                new_dict = {} 
                new_dict['Municipality'] = feature['properties']['municipality']
                new_dict['Station Name'] = feature['properties']['station']
                new_dict["Num Docks"] = feature['properties']['of_docks']
                new_dict['Longitude'] = feature['properties']['longitude']
                new_dict['Latitude'] = feature['properties']['latitude']
                hubway_list.append(new_dict)
        #print(hubway_list)

        x = boston_hubway_stations.project(hubway_list, lambda t: (t['Municipality'], t['Num Docks']))

        num_docks_per_municipality = boston_hubway_stations.aggregate(x, sum)
        num_docks_per_municipality = [{'# of Docks': n, 'Municipality': m} for (m,n) in num_docks_per_municipality]
        print(num_docks_per_municipality)

        repo['jtbloom_rfballes.boston_hubway_stations'].insert_many(num_docks_per_municipality)


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

#boston_hubway_stations.execute()