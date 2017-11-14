import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class neighborhood_income(dml.Algorithm):
    contributor = 'jtbloom_rfballes_medinad'
    reads = ['jtbloom_rfballes_medinad.neighborhood_income']
    writes = ['jtbloom_rfballes_medinad.neighborhood_income2']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')      

        repo.dropCollection("jtbloom_rfballes_medinad.neighborhood_income2")
        repo.createCollection("jtbloom_rfballes_medinad.neighborhood_income2")

        neighborhood_list = []

        for item in repo.jtbloom_rfballes_medinad.neighborhood_income.find():
                for i in item['results']:
                        #print(i)
                        new_dict = {}
                        new_dict['Neighborhood'] = i['Neighborhood']
                        new_dict['Income'] = i['Per Capita Income ']
                        neighborhood_list.append(new_dict)
        print(neighborhood_list)

        


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

neighborhood_income.execute()