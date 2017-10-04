import json
import datetime
import dml
import prov.model
import uuid
import sys
import pandas as pd
from pprint import pprint
from bson import ObjectId

### This transformation combines the MeansOfCommuting, PovertyRates, and Race datasets
###
### Relevant commuting columns: % columns relating to use of public transit
### Relevant poverty rates columns: poverty rate %, % of Boston's impoverished
### Relevant race columns: % of each race in every neighborhood
###
### Relationships being explored: In each neighborhood of Boston, use the social
### demographics to determine if there is a correlation to how one relies on 
### commuting to work. Additional areas for exploration: do these social demographics
### play a role in the accessibility of the MBTA

class demographics_versus_mbta(dml.Algorithm):

	contributor = 'nathansw_sbajwa'
	reads = ['nathansw_sbajwa.commuting', 'nathansw_sbajwa.povertyrates', 'nathansw_sbajwa.race']
	writes = ['nathansw_sbajwa.demographics_versus_commuting']

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		# open db client and authenticate
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('nathansw_sbajwa', 'nathansw_sbajwa')

		############### COMMUTING ################################

		commuting_db = repo['nathansw_sbajwa.commuting']
		commuting = commuting_db.find_one()

		commuting_data = {}
		commuting_cols = ['Bus or trolley %', 'Railroad %', 'Subway or elevated %']

		## Aggregate all commuting data related to MBTA and summarize in one column
		for town in commuting:
			if town == 'Massachusetts' or town == 'Boston':
				continue
			if town =='_id':
				continue
			commuting_data[town] = {}

			# string manipulation, typecast to float in order to perform math ops on values
			bus = float(commuting[town][commuting_cols[0]].replace('%', ''))
			railroad = float(commuting[town][commuting_cols[1]].replace('%', ''))
			subway = float(commuting[town][commuting_cols[2]].replace('%', ''))
			transit_sum = round(bus + railroad + subway, 2)
			commuting_data[town]['Commute with Public Transit %'] = transit_sum

		commuting_data = json.dumps(commuting_data, indent=4)
		json_commuting = json.loads(commuting_data)

		################ POVERTY ###################################

		pov_db = repo['nathansw_sbajwa.povertyrates']
		pov_rates = pov_db.find_one()

		pov_data = {}
		pov_cols = ['Poverty rate', "Percent of Boston's\nimpoverished"]

		for town in pov_rates:
			if town == 'Massachusetts' or town == 'Boston':
				continue
			if town == "_id":
				continue
			pov_data[town] = {}
			pov_data[town]['Poverty Rate %'] = float(pov_rates[town][pov_cols[0]].replace('%',''))
			pov_data[town]["Percentage of Boston's Impoverished"] = float(pov_rates[town][pov_cols[1]].replace('%',''))

		pov_data = json.dumps(pov_data, indent=4)
		json_pov = json.loads(pov_data)

		########################## RACE ###################################

		race_db = repo['nathansw_sbajwa.race']
		races = race_db.find_one()

		race_data = {}

		for town in races:
			if town == 'Massachusetts' or town == 'Boston':
				continue
			if town == "_id":
				continue
			race_data[town] = {}
			for key in races[town]:
				if '%' in key:
					# Entries with '-' indicate too small of a percentage reported
					if '-' in races[town][key]:
						race_data[town][key] = 0.0
						continue
					race_data[town][key] = float(races[town][key].replace('%', ''))

		race_data = json.dumps(race_data, indent=4)
		json_race = json.loads(race_data)

		##############################################################################

		commuting_df = pd.DataFrame(json_commuting)
		pov_df = pd.DataFrame(json_pov)
		race_df = pd.DataFrame(json_race)

		frames = [commuting_df, pov_df, race_df]
		result = pd.concat(frames, join='outer')
		result = pd.DataFrame.transpose(result)

		# Aggregate column data for all non-white minorities into one column in order to see 
		# any relationships that race is involved in more clearly
		result['Non-white Minorities %'] = result['Asian %'] + result['Black or AfricanAmerican %'] + \
			result['Hispanic or Latino %'] + result['Other %'] 

		## write to csv file for debugging purposes
		# result.to_csv('algorithm3.csv', encoding='utf-8')

		test = result.to_dict('index')
		agg_data = json.dumps(test, indent=4)
		merged_data = json.loads(agg_data)

		repo.dropCollection('demographics_versus_mbta')
		repo.createCollection('demographics_versus_mbta')
		repo['nathansw_sbajwa.demographics_versus_mbta'].insert_one(merged_data)

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		return doc