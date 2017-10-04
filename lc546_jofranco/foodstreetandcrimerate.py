import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class foodstreedandcrimerate(dml.Algorithm):
	contributor = 'lc546_jofranco'
	reads = []
	writes = ['lc546_jofranco.foodstreet_andcrimes']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.autenticate("lc546_jofranco", "lc546_jofranco")
		crimerateData = lc546_jofranco.crime_rate
		foodpermitData = lc546_jofranco.restaurant_permit
