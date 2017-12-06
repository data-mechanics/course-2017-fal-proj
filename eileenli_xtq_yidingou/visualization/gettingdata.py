import urllib.request
import json
import dml
import datetime
import uuid
import requests
import numpy as np
import math
import prov.model

# visual1 data function
class gettingdata(dml.Algorithm):
	contributor = 'eileenli_xtq_yidingou'
	reads = ['eileenli_xtq_yidingou.schoolfinal']
	writes = []
	
	@staticmethod
	def execute():
		return gettingdata.get_data()


	@staticmethod
	def get_data(trail=False):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')

		# loads
		schoolfinal = repo['eileenli_xtq_yidingou.schoolfinal'].find()

		sch_cord=[]
		for i in schoolfinal:
			for j in i["properties"]:
				try:
					sch_cord.append(j['coordinates'])
				except:
					pass


# 		X = np.array(obesity_time_tuples)[:,0]
#  Y that will be returned (obesity percentage)
# 		Y = np.array(obesity_time_tuples)[:,1] 
# # Population array 
# 		Pop = np.array(obesity_time_tuples)[:,2]

# # linear regression code
# 		meanX = sum(X)*1.0/len(X)
# 		meanY = sum(Y)*1.0/len(Y)

# 		varX = sum([(v-meanX)**2 for v in X])
# 		varY = sum([(v-meanY)**2 for v in Y])

# 		minYHatCov = sum([(X[i]-meanX)*(Y[i]-meanY) for i in range(len(Y))])

# 		B1 = minYHatCov/varX
# 		B0 = meanY - B1*meanX

# 		yhat = []
# 		for i in range(len(X)):
# 				yhat += [B0 + (X[i]*B1)]

# 		data = []
# 		for i in range(len(Y)):
# 				data += [{"yhat": yhat[i],
# 					"y": Y[i],
# 					"x": X[i],
# 					"population": Pop[i]}]

		return sch_cord


	@staticmethod
	def provenance():
		pass
gettingdata.execute()