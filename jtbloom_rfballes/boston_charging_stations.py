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
        new_dict = {}

        for item in new_charging_stations:
            for feature in item['features']: 
                    city = feature['properties']['CITY']
                    station_name = feature['properties']['STATION_NA']
                    address = feature['properties']['ADDRESS']
                    longitude = feature['properties']['LONGITUDE']
                    latitude = feature['properties']['LATITDE']
                    if (city == 'Boston'):
                        new_dict['City'] = city
                        new_dict['Station Name'] = station_name
                        new_dict['Address'] = address
                        new_dict['Longitude'] = longitude
                        new_dict['Latitude'] = latitude
                        print(new_dict)
        
        repo['jtbloom_rfballes.boston_charging_stations'].insert(new_dict)






    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

boston_charging_stations.execute()