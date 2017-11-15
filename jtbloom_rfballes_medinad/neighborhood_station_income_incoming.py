import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class neighborhood_station_income_incoming(dml.Algorithm):
    contributor = 'jtbloom_rfballes_medinad'
    reads = ['jtbloom_rfballes_medinad.stationsByneighborhood', 'jtbloom_rfballes_medinad.outgoing_trips', ]
    writes = ['jtbloom_rfballes_medinad.neighborhood_station_income_out']

	def intersect(R, S):
		return [t for t in R if t in S]

	def project(R, p):
    return [p(t) for t in R]


    @staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

		repo.dropCollection('jtbloom_rfballes_medinad.neighborhood_station_income_out')
		repo.createCollection('jtbloom_rfballes_medinad.neighborhood_station_income_out')

		s_by_n = list(repo['jtbloom_rfballes_medinad.stationsByneighborhood'].find())
		out = list(repo['jtbloom_rfballes_medinad.outgoing_trips'].find())


		for i in range(len())
