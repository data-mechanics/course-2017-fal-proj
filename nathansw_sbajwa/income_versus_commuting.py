import json
import datetime
import dml
import prov.model
import uuid
import sys
import pandas as pd
from pprint import pprint
from bson import ObjectId

#### This transformation combines the HouseholdIncome datasat and MeansOfCommuting dataset
#### First, we only grab the relevant data from each dataset
#### Relevant household income columns: median income, total households, and every income bracket % (instead of person count)
#### Relevant commuting columns: Total amount in the workforce, amount that work from home, and every transportation bracket %
#### Relationship being explored: Relationship between different ranges of household income
#### in each Boston neighborhood and the reliance on public transportation as a way
#### of commuting to work

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('nathansw_sbajwa', 'nathansw_sbajwa')

def start_transformation():
	startTime = datetime.datetime.now()
	db1 = repo['nathansw_sbajwa.householdincome']
	incs = db1.find_one()

	inc_cols = ['Median Income', 'Total Households']
	inc_data = {}	# will hold only the data specified above

	for town in incs.keys():
		if town == 'Massachusetts' or town == 'Harbor Islands':
			continue
		if town == '_id': 
			continue
		inc_data[town] = {}
		for key, value in incs[town].items():
			if key in inc_cols or '%' in key:
				inc_data[town][key] = value
		
	inc_data = json.dumps(inc_data, indent=4)	 #string object
	json_incs = json.loads(inc_data)	#json/dict object

	db2 = repo['nathansw_sbajwa.commuting']
	comms = db2.find_one()
	# cursor = db2.find({})
	# for doc in cursor:
	# 	pprint(doc)

	comm_cols = ['Workers 16\nyears and over', 'Worked \nat home']
	comm_data = {}

	for town in comms:
		if town == 'Massachusetts' or town == 'Harbor Islands':
			continue
		if town == '_id':
			continue
		comm_data[town] = {}
		for key, value in comms[town].items():
			if key in comm_cols or '%' in key:
				comm_data[town][key] = value

	comm_data = json.dumps(comm_data, indent = 4)	#string object
	json_comms = json.loads(comm_data)	#json/dict object

	# load both json object as a panda dataframe 
	df1 = pd.DataFrame(json_incs)
	df2 = pd.DataFrame(json_comms)

	# concatenate the two frames
	frames = [df1, df2]
	result = pd.concat(frames)

	# switch the rows and columns
	# original setup: columns = neighborhoods
	# new setup: rows = neighborhoods 
	## allowing the different categories of information to be columns will make it easier to query and search for what we want
	result = pd.DataFrame.transpose(result)	

	### For household income, this is how we are aggregating the data. Each array represents the columns in which the values
	### will be aggregated and stored in one column 
	agg_inc_cols = [['<14,999 %', '15,000-24,999 %', '25,000-34,999 %'], \
	['35,000-49,999 %', '50,000-74,999 %'], ['75,000-99,999 %', \
	'100,000-149,000 %', '>=150,000 %']]

	for group in agg_inc_cols:	#group = each list within agg_inc_cols	
		for col in group:	#col = each entry within each 'group'
			result[col] = (result[col].str.replace('%', ''))	#remove the % sign from all data and convert it to a float so we can perform math ops later
			result[col] = result[col].astype(float)


	new_inc_cols = ['<34,999 %', '35,000-74,999 %', '>=75,000 %']	# names of new columns (corelate to the lists in agg_inc_cols)
	idx = 0

	# This for loop basically sums all of the numbers in the groups of columns specified above and stores the values in the new column (see new_inc_cols)
	# for example: each neighborhood's household income % numbers in columns $75,000-$99,999 %, $100,000-$149,000 %, and >=$150,000 % will be summed together
	# and stored in a new column entitled >=75,000 %
	for group in agg_inc_cols:
		new_name = new_inc_cols[idx]
		result[new_name] = 0
		for col in group:
			result[new_name] += result[col]
			result.drop(col, axis=1, inplace=True)
		idx += 1

	# For commuting data, we are only aggregating the data in columns that relate to the use of public transporation
	# a similar process is followed
	agg_com_cols = ['Bus or trolley %', 'Railroad %', 'Subway or elevated %']

	result['Public transit %'] = 0
	for col in agg_com_cols:
		result[col] = (result[col].str.replace('%', ''))
		result[col] = result[col].astype(float)
		result['Public transit %'] += result[col]
		result.drop(col, axis=1, inplace=True)

	# ## write to csv for testing purposes
	# result.to_csv('algorithm1.csv', encoding='utf-8')

	test = result.to_dict('index')
	agg_data = json.dumps(test, indent=4)
	merged_data = json.loads(agg_data)

	repo.dropPermanent('income_verses_commuting')
	repo.createPermanent('income_verses_commuting')
	repo['nathansw_sbajwa'].insert_one(merged_data)

	endTime = datetime.datetime.now()
	

start_transformation()