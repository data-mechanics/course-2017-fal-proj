import json
import time
import pprint 
import datetime
import dml
import prov.model
import uuid
import pandas as pd
import numpy as np
from bson import ObjectId
from urllib.request import urlopen
from  itertools import combinations , combinations_with_replacement , product
import pprint

### Algorithm 2

class trickleAnalysis(dml.Algorithm):
  def return_distance(row, neighborhood_distance_matrix):
    return neighborhood_distance_matrix.loc[row.neighborhood1,row.neighborhood2]

  def cal_neighborhood_diff(x,ref_df):
    if((x['neighborhood2'] in ref_df.index) & (x['neighborhood1'] in ref_df.index)):
      return((ref_df[x['neighborhood1']] - ref_df[x['neighborhood2']] ))
    else:
      return np.nan

  def get_corr(x,correlation_col,neighborhood_pair):
    neighborhood_name = x['neighborhood']
    filtered_df = neighborhood_pair[(neighborhood_pair['neighborhood1']==neighborhood_name) & neighborhood_pair['is_neighbor']==1]
    return filtered_df['neighborhood_distance'].corr(filtered_df[correlation_col])

  contributor = 'nathansw_rooday_sbajwa_shreyap'
  reads = ['nathansw_rooday_sbajwa_shreyap.trickling', 'nathansw_rooday_sbajwa_shreyap.neighborhoodMap', 'nathansw_sbajwa.householdincome', 'nathansw_sbajwa.povertyrates', 'nathansw_sbajwa.commuting']
  writes = ['nathansw_rooday_sbajwa_shreyap.trickleAnalysis']

  @staticmethod
  def execute(trial=False):
    startTime = datetime.datetime.now()
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')
    
    # Five db uses
    trickle_db = repo['nathansw_rooday_sbajwa_shreyap.trickling']
    neighborhoodMap_db = repo['nathansw_rooday_sbajwa_shreyap.neighborhoodMap']
    householdincome_db = repo['nathansw_sbajwa.householdincome']
    povertyrates_db = repo['nathansw_sbajwa.povertyrates']
    commuting_db = repo['nathansw_sbajwa.commuting']

    print("Loading Trickle Data")
    trickle_data = trickle_db.find_one()
    del trickle_data['_id']
    neighborhood_distance_matrix = pd.DataFrame.from_dict(trickle_data)
    neighborhood_distance_matrix.index = neighborhood_distance_matrix.neighborhood

    print("Creating neighborhood pairs")
    unique_neighborhood = neighborhood_distance_matrix.index.tolist()
    neighborhood_pair = pd.DataFrame(list(product(unique_neighborhood,repeat=2)))
    neighborhood_pair.columns = ['neighborhood1','neighborhood2']
    neighborhood_pair = neighborhood_pair[neighborhood_pair['neighborhood1'] != neighborhood_pair['neighborhood2']]
    neighborhood_pair['neighborhood_distance'] = neighborhood_pair.apply(trickleAnalysis.return_distance,args=(neighborhood_distance_matrix,), axis=1)

    print("Loading Neighborhood Map Data")
    neighborhoodMap_data = neighborhoodMap_db.find_one()
    del neighborhoodMap_data['_id']
    neighborhood = pd.DataFrame([(key, x) for key,val in neighborhoodMap_data.items() for x in val], columns=['neighborhood1', 'neighborhood1'])
    neighborhood.columns = ['neighborhood1','neighborhood2']
    neighborhood['is_neighbor']=1

    print("Merging Neighborhood Pairs with Neighborhood data")
    neighborhood_pair = neighborhood_pair.merge(neighborhood,left_on=['neighborhood1','neighborhood2'],right_on=['neighborhood1','neighborhood2'],how='left')#[['neighborhood1','neighborhood2','is_neighbor_y']]
    neighborhood_pair.columns = ['neighborhood1','neighborhood2','neighborhood_distance','is_neighbor']
    neighborhood_pair = neighborhood_pair.fillna(0)
    
    print("Loading Poverty Rates Data")
    povertyrates_data = povertyrates_db.find_one()
    del povertyrates_data['_id']
    PovertyRates = pd.DataFrame.from_dict(povertyrates_data).transpose()
    PovertyRates.columns = 'Income_' + PovertyRates.columns
    poverty_rate = PovertyRates['Income_Poverty rate']
    poverty_rate = poverty_rate.apply(lambda x: float(x.split("%")[0]))

    print("Calculating Poverty Percentage Difference")
    neighborhood_pair['poverty_percentage_diff'] = neighborhood_pair.apply(trickleAnalysis.cal_neighborhood_diff,args=([poverty_rate]),axis=1)
    neighborhood_trickling_effect = pd.DataFrame({
      'neighborhood': unique_neighborhood
    })
    neighborhood_trickling_effect.index = neighborhood_trickling_effect.neighborhood
    correlation_col = 'poverty_percentage_diff'
    neighborhood_trickling_effect['poverty_trickling_effect'] = neighborhood_trickling_effect.apply(trickleAnalysis.get_corr, args=(['poverty_percentage_diff', neighborhood_pair]),axis=1)
    
    print("Loading Household Income Data")
    householdincome_data = householdincome_db.find_one()
    del householdincome_data['_id']
    HouseholdIncome = pd.DataFrame.from_dict(householdincome_data).transpose()
    HouseholdIncome.columns = 'Income_' + HouseholdIncome.columns

    print("Calculating Median Income Difference")
    median_income = HouseholdIncome['Income_Median Income']
    median_income = median_income.apply(lambda x: float(x.replace(",","").replace("$","").replace("-","0") ))
    neighborhood_pair['median_income_diff'] = neighborhood_pair.apply(trickleAnalysis.cal_neighborhood_diff,args=([median_income]),axis=1)
    neighborhood_trickling_effect['median_income_effect'] = neighborhood_trickling_effect.apply(trickleAnalysis.get_corr, args=(['median_income_diff', neighborhood_pair]),axis=1)

    print("Loading Commute Data")
    commuting_data = commuting_db.find_one()
    del commuting_data['_id']
    MeansOfCommuting = pd.DataFrame.from_dict(commuting_data).transpose()

    print("Calculating Bike Commute Difference")
    bike_commute = MeansOfCommuting['Bicycle %']
    bike_commute = bike_commute.apply(lambda x: float(x.replace("%","")))
    neighborhood_pair['bike_commute_diff'] = neighborhood_pair.apply(trickleAnalysis.cal_neighborhood_diff,args=([bike_commute]),axis=1)
    neighborhood_trickling_effect['bike_commute_effect'] = neighborhood_trickling_effect.apply(trickleAnalysis.get_corr, args=(['bike_commute_diff', neighborhood_pair]),axis=1)

    print("Calculating Walk Commute Difference")
    walk_commute = MeansOfCommuting['Walked %']
    walk_commute = walk_commute.apply(lambda x: float(x.replace("%","")))
    neighborhood_pair['walk_commute_diff'] = neighborhood_pair.apply(trickleAnalysis.cal_neighborhood_diff,args=([walk_commute]),axis=1)
    neighborhood_trickling_effect['walk_commute_effect'] = neighborhood_trickling_effect.apply(trickleAnalysis.get_corr, args=(['walk_commute_diff', neighborhood_pair]),axis=1)

    print("Calculating Bus Commute Difference")
    bus_commute = MeansOfCommuting['Bus or trolley %']
    bus_commute = bus_commute.apply(lambda x: float(x.replace("%","")))
    neighborhood_pair['bus_commute_diff'] = neighborhood_pair.apply(trickleAnalysis.cal_neighborhood_diff,args=([bus_commute]),axis=1)
    neighborhood_trickling_effect['bus_commute_effect'] = neighborhood_trickling_effect.apply(trickleAnalysis.get_corr, args=(['bus_commute_diff', neighborhood_pair]),axis=1)

    print("Calculating Car Commute Difference")
    car_commute = MeansOfCommuting['Car, truck, or van %']
    car_commute = car_commute.apply(lambda x: float(x.replace("%","")))
    neighborhood_pair['car_commute_diff'] = neighborhood_pair.apply(trickleAnalysis.cal_neighborhood_diff,args=([car_commute]),axis=1)
    neighborhood_trickling_effect['car_commute_effect'] = neighborhood_trickling_effect.apply(trickleAnalysis.get_corr, args=(['car_commute_diff', neighborhood_pair]),axis=1)

    finalTrickleData = neighborhood_trickling_effect.to_dict()    
    del finalTrickleData['neighborhood']

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(finalTrickleData)
    
    print("Saving Trickle Analysis Data")
    repo.dropCollection('trickleAnalysis')
    repo.createCollection('trickleAnalysis')
    repo['nathansw_rooday_sbajwa_shreyap.trickleAnalysis'].insert_one(finalTrickleData)
    
    print("Done!")
    repo.logout()
    endTime = datetime.datetime.now()
    return {"start":startTime, "end":endTime}

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime=None, endTime=None):
    client = dml.pymongo.MongoClient()
    repo = client.repo

    ##########################################################

    ## Namespaces
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nathansw_rooday_sbajwa_shreyap/') # The scripts in / format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap/') # The data sets in / format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

    doc.add_namespace('dat2', 'http://datamechanics.io/data/nathansw_sbajwa/')

    ## Agents
    this_script = doc.agent('alg:nathansw_rooday_sbajwa_shreyap#trickleAnalysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ## Activities
    get_trickleAnalysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

    ## Entitites
    # Data Source
    resource1 = doc.entity('dat:trickling.json', {'prov:label':'Trickle', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource2 = doc.entity('dat:neighborhood_map.json', {'prov:label':'Neighborhood Map', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource3 = doc.entity('dat2:HouseholdIncome.json', {'prov:label':'Household Income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource4 = doc.entity('dat2:PovertyRates.json', {'prov:label':'Poverty Rates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource5 = doc.entity('dat2:MeansOfCommuting.json', {'prov:label':'Commuting', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    # Data Generated
    trickleAnalysis = doc.entity('dat:nathansw_rooday_sbajwa_shreyap#trickleAnalysis', {prov.model.PROV_LABEL:'Trickle Analysis', prov.model.PROV_TYPE:'ont:DataSet'})
       
    ############################################################

    ## wasAssociatedWith      
    doc.wasAssociatedWith(get_trickleAnalysis, this_script)

    ## used
    doc.usage(get_trickleAnalysis, resource1, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})
    doc.usage(get_trickleAnalysis, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})
    doc.usage(get_trickleAnalysis, resource3, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})
    doc.usage(get_trickleAnalysis, resource4, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})
    doc.usage(get_trickleAnalysis, resource5, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',})

    ## wasGeneratedBy
    doc.wasGeneratedBy(trickleAnalysis, get_trickleAnalysis, endTime)

    ## wasAttributedTo
    doc.wasAttributedTo(trickleAnalysis, this_script)

    ## wasDerivedFrom
    doc.wasDerivedFrom(trickleAnalysis, resource1, get_trickleAnalysis, get_trickleAnalysis, get_trickleAnalysis)
    doc.wasDerivedFrom(trickleAnalysis, resource2, get_trickleAnalysis, get_trickleAnalysis, get_trickleAnalysis)
    doc.wasDerivedFrom(trickleAnalysis, resource3, get_trickleAnalysis, get_trickleAnalysis, get_trickleAnalysis)
    doc.wasDerivedFrom(trickleAnalysis, resource4, get_trickleAnalysis, get_trickleAnalysis, get_trickleAnalysis)
    doc.wasDerivedFrom(trickleAnalysis, resource5, get_trickleAnalysis, get_trickleAnalysis, get_trickleAnalysis)

    ############################################################

    repo.logout()

    return doc