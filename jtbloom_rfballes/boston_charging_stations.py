import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class boston_charging_stations(dml.Algorithm):
    contributor = 'jtbloom_rfballes'
    reads = ['jtbloom_rfballes.charging_stations']
    writes = ['jtbloom_rfballes.boston_charging_stations']

    @staticmethod
    def execute(trial = False):
        pass
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes', 'jtbloom_rfballes')

        repo.dropCollection("jtbloom_rfballes.boston_charging_stations")
        repo.createCollection("jtbloom_rfballes.boston_charging_stations")

        new_charging_stations = repo.jtbloom_rfballes.charging_stations.find()
        
        charging_list = []
        
        for item in new_charging_stations:
            for feature in item['features']: 
                new_dict = {}
                if (feature['properties']['CITY'] == 'Boston'):
                    new_dict['City'] = feature['properties']['CITY']
                    new_dict['Station Name'] = feature['properties']['STATION_NA']
                    new_dict['Address'] = feature['properties']['ADDRESS']
                    new_dict['Longitude'] = feature['properties']['LONGITUDE']
                    new_dict['Latitude'] = feature['properties']['LATITDE']
                    charging_list.append(new_dict)
        #print(charging_list)
        repo['jtbloom_rfballes.boston_charging_stations'].insert_many(charging_list)

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

#boston_charging_stations.execute()