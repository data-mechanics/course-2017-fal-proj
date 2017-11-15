import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt
import decimal


class incoming_outgoing(dml.Algorithm):
	contributor = 'jtbloom_rfballes_medinad'
	reads = ['jtbloom_rfballes_medinad.incoming_trips', 'jtbloom_rfballes_medinad.outgoing_trips']
	writes = ['jtbloom_rfballes_medinad.incoming_outgoing']

	
	def project(tripset, item):
		return [item(t) for t in tripset]	

	def product(R, S):
		return [(t,u) for t in R for u in S]

	def aggregate(R, f):
		keys = {r[0][0] for r in R}
		return [(key, f([v for ((k,v), lat, lon) in R if k == key])) for key in keys]	

	def count_trips(iterable):
		num = 0
		for i in iterable:
			num += 1
		return num 

	def intersect(R, S):
		return [t for t in R if t in S]

	def permute(x):
		shuffled = [xi for xi in x]
		shuffle(shuffled)
		return shuffled

	def avg(x): # Average
		return sum(x)/len(x)

	def stddev(x): # Standard deviation.
		m = incoming_outgoing.avg(x)
		return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

	def cov(x, y): # Covariance.
		return sum([(xi-incoming_outgoing.avg(x))*(yi-incoming_outgoing.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

	def corr(x, y): # Correlation coefficient.
		if incoming_outgoing.stddev(x)*incoming_outgoing.stddev(y) != 0:
			return incoming_outgoing.cov(x, y)/(incoming_outgoing.stddev(x)*incoming_outgoing.stddev(y))

	def p(x, y):
		c0 = incoming_outgoing.corr(x, y)
		corrs = []
		for k in range(0, 2000):
			y_permuted = incoming_outgoing.permute(y)
			corrs.append(incoming_outgoing.corr(x, y_permuted))
		return len([c for c in corrs if abs(c) > c0])/len(corrs)



	@staticmethod
	def execute(trial = False):

		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()


    	# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

		repo.dropCollection('jtbloom_rfballes_medinad.incoming_outgoing')
		repo.createCollection('jtbloom_rfballes_medinad.incoming_outgoing')


		

		inc = repo.jtbloom_rfballes_medinad.incoming_trips.find()
		out = repo.jtbloom_rfballes_medinad.outgoing_trips.find()


		inc_out_list = []

		in_l = []
		out_l = []

		for i in inc:
			in_l.append(i)
		for j in out: 
			out_l.append(j)

		inter = []
		for k in range(len(in_l)):
			for l in range(len(in_l)):
				if in_l[k]['end station'] == out_l[l]['station']:
					inter.append([in_l[k],out_l[l]])

		print(inter[:3])



		inct = []
		outt = []
		for i in inter:
			#print(i[0]['# incoming trips'])
			inct.append(i[0]['# incoming trips'])
			outt.append(i[1]['# outgoing trips'])

		print(inct)

		pval = incoming_outgoing.p(inct,outt)

		correl = incoming_outgoing.corr(inct,outt)

		covarr = incoming_outgoing.cov(inct,outt)

		stdevi = incoming_outgoing.stddev(inct)

		stdevo = incoming_outgoing.stddev(outt)

		avgi = incoming_outgoing.avg(inct)



		print("Incoming Avg: "+ str(avgi))
		print("Incoming Stdev: "+ str(stdevi) + " Outgoing Stdev: "+ str(stdevo))
		print("Covariance: ", covarr)
		print("Correlation: ", correl)
		print("P-Value: ",decimal.Decimal(pval))
	

		print(len(out_l))
		

		inter = [{'features':x} for x in inter]

		repo['jtbloom_rfballes_medinad.incoming_outgoing'].insert_many(inter)

			


		@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dm', 'http://datamechanics.io/data/jb_rfb_dm_proj2data/')
        
        this_script = doc.agent('dm:jtbloom_rfballes_medinad#incoming_outgoing', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        incoming_resource = doc.entity('nei:incoming_outgoing', {'prov:label':'Incoming Hubway Trips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})
		outgoing_resource = doc.entity('nei:incoming_outgoing', {'prov:label':'Outgoing Hubway Trips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})
		incoming_outgoing_resource = = doc.entity('nei:incoming_outgoing', {'prov:label':'Incoming/Outgoing Hubway Trips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'DataSet'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(this_run this_script)
 
        doc.usage(this_run, incoming_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

         doc.usage(this_run, outgoing_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        doc.wasAttributedTo(incoming_outgoing_resource, this_script)

        doc.wasGeneratedBy(incoming_outgoing_resource, this_run, endTime)

        doc.wasDerivedFrom(incoming_outgoing_resource, incoming_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(incoming_outgoing_resource, outgoing_resource, this_run, this_run, this_run)


        repo.logout()
                  
        return doc

#incoming_outgoing.execute()
