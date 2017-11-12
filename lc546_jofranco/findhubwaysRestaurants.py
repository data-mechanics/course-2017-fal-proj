import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class findHubwaysRestaurants(dml.Algorithm):
	contributor = 'lc546_jofranco'
	reads = ['lc546_jofranco.hubwaybike', 'lc546_jofranco.permit']
	writes = ['lc546_jofranco.CrimeRestaurants']
	@staticmethod
	def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        '''Find list of restaurants that are near the hubway stations'''
        hubs = repo.lc546_jofranco.hubwaybike
        #food = repo.lc546_jofranco.permit
        restaurants_near_hubway = []
        for i in hubs.find():
            restaurants = len(repo.command('geoNear', 'lc546_jofranco.permit' near = (str(i["la"]) + str(i["lo"])), spherical =True, maxDistance = )['results'])


        endTime = datetime.datetime.now()

    return {"start":startTime, "end":endTime}
