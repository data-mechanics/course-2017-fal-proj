import json
import datetime
import dml
import prov.model
import uuid
import sys
import pandas as pd
from pprint import pprint
from bson import ObjectId

### This transformation combines the HouseholdIncome datasat and MeansOfCommuting dataset
###
### First, we only grab the relevant data from each dataset
### Relevant household income columns: median income, total households, and every income bracket % (instead of person count)
### Relevant commuting columns: Total amount in the workforce, amount that work from home, and every transportation bracket %
###
### Relationship being explored: Relationship between different ranges of household income
### in each Boston neighborhood and the reliance on public transportation as a way
### of commuting to work

class income_versus_commuting(dml.Algorithm):

	contributor = 'nathansw_sbajwa'
	reads = ['nathansw_sbajwa.householdincome', 'nathansw_sbajwa.commuting']
	writes = ['nathansw_sbajwa.income_versus_commuting']	

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		# open db client and authenticate
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa', 'nathansw_sbajwa')

		############### HOUSEHOLD INCOME ##########################

		householdincome_db = repo['nathansw_sbajwa.householdincome']
		income = householdincome_db.find_one()

		income_cols = ['Median Income', 'Total Households']
		income_data = {}	# will hold only the data specified above

		for town in income.keys():
			if town == 'Massachusetts' or town == 'Harbor Islands':
				continue
			if town == '_id': 
				continue
			income_data[town] = {}
			for key, value in income[town].items():
				if key in income_cols or '%' in key:
					income_data[town][key] = value
			
		income_data = json.dumps(income_data, indent=4)	 #string object
		json_income = json.loads(income_data)			 #json/dict object

		#################### COMMUTING ##############################

		commuting_db = repo['nathansw_sbajwa.commuting']
		commuting = commuting_db.find_one()

		commuting_cols = ['Workers 16\nyears and over', 'Worked \nat home']
		commuting_data = {}

		for town in commuting:
			if town == 'Massachusetts' or town == 'Harbor Islands':
				continue
			if town == '_id':
				continue
			commuting_data[town] = {}
			for key, value in commuting[town].items():
				if key in commuting_cols or '%' in key:
					commuting_data[town][key] = value

		commuting_data = json.dumps(commuting_data, indent = 4)	#string object
		json_commuting = json.loads(commuting_data)				#json/dict object

		################################################################

		# load both json object as a panda dataframe 
		income_df = pd.DataFrame(json_income)
		commuting_df = pd.DataFrame(json_commuting)

		# concatenate the two frames
		frames = [income_df, commuting_df]
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

		for group in agg_inc_cols:									# group = each list within agg_inc_cols	
			for col in group:										# col = each entry within each 'group'
				result[col] = (result[col].str.replace('%', ''))	# remove the % sign from all data and convert it to a float so we can perform math ops later
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

		test = result.to_dict('index')
		agg_data = json.dumps(test, indent=4)
		merged_data = json.loads(agg_data)

		repo.dropCollection('income_versus_commuting')
		repo.createCollection('income_versus_commuting')
		repo['nathansw_sbajwa.income_versus_commuting'].insert_one(merged_data)

		repo.logout()

		endTime = datetime.datetime.now()
	
		return {"start":startTime, "end":endTime}

	### WIP ###
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa','nathansw_sbajwa')

		## Namespaces
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/sbajwa_nathansw/') # The scripts in / format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/sbajwa_nathansw/') # The data sets in / format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

		doc.add_namespace('householdincome', 'dat:nathansw_sbajwa#householdincome')
		doc.add_namespace('commuting', 'dat:nathansw_sbajwa#commuting')

		## Agents
		this_script = doc.agent('alg:nathansw_sbajwa#income_versus_commuting', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		## Activities
		get_householdincome = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_commuting = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		## Entitites
		resource1 = doc.entity('dat:nathansw_sbajwa#householdincome', {'prov:label':'Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource2 = doc.entity('commuting: dat:nathansw_sbajwa#commuting', {'prov:label':'Means of Commuting by Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		householdincome = doc.entity('dat:nathansw_sbajwa#householdincome', {prov.model.PROV_LABEL:'Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		commuting = doc.entity('dat:nathansw_sbajwa#commuting', {prov.model.PROV_LABEL:'Means of Commuting by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})

		## wasAssociatedWith
		doc.wasAssociatedWith(get_householdincome, this_script)
		doc.wasAssociatedWith(get_commuting, this_script)

		## used
		doc.usage(get_householdincome, resource1, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(get_commuting, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

		## wasGeneratedBy
		doc.wasGeneratedBy(householdincome, get_householdincome, endTime)
		doc.wasGeneratedBy(commuting, get_commuting, endTime)

		## wasAttributedTo
		doc.wasAttributedTo(householdincome, this_script)
		doc.wasAttributedTo(commuting, this_script)

		## wasDerivedFrom
		doc.wasDerivedFrom(householdincome, resource1, get_householdincome, get_householdincome, get_householdincome)
		doc.wasDerivedFrom(commuting, resource2, get_commuting, get_commuting, get_commuting)	

		repo.logout()

		return doc