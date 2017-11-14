import json
from urllib.request import urlopen
import time
import pprint 
import datetime
import dml
import prov.model
import uuid
from bson import ObjectId
import pandas as pd
import numpy as np
import seaborn
from sklearn.model_selection import cross_val_predict
from sklearn import linear_model
from sklearn import metrics
import sklearn
import statsmodels.api as sm
import statsmodels

### Algorithm 1

class Regression_Analysis(dml.Algorithm):

  contributor = 'nathansw_rooday_sbajwa_shreyap'
  ### Make sure this is the correct dataset file name
  reads = ['nathansw_rooday_sbajwa_shreyap.otpByLine']
    
  # Currently it just creates a csv file 
  writes = ['nathansw_rooday_sbajwa_shreyap.regressionAnalysis']

  @staticmethod
  def execute(trial=False):

    startTime=datetime.datetime.now()
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')
    
    print("")
    # Read data from mongo
    mbta_db = repo['nathansw_rooday_sbajwa_shreyap.otpByLine']

    # Read data into pandas
    data = mbta_db.find_one()
    del data['_id']
    otp_by_line =  pd.DataFrame.from_dict(data)
   



    otp_by_line = otp_by_line.transpose()
    otp_by_line[otp_by_line['Peak Service']==''] = np.nan
    otp_by_line[otp_by_line['Off-Peak Service']==''] = np.nan


    print(otp_by_line)
    

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime=None, endTime=None):
    client = dml.pymongo.MongoClient()
    repo = client.repo

    ##########################################################

    ## Namespaces
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/sbajwa_nathansw/') # The scripts in / format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/sbajwa_nathansw/') # The data sets in / format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


    """
    ## Agents
    this_script = doc.agent('alg:nathansw_rooday_sbajwa_shreyap#mbta_stops_lines', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ## Activities
    get_mbta_stops_lines = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

    ## Entitites
    # Data Source
    resource = 
    # Data Generated
    mbta_stops_lines = 
       
    ############################################################

        ## wasAssociatedWith      

    ## used   

    ## wasGeneratedBy

    ## wasAttributedTo    

    ## wasDerivedFrom

    ############################################################
    """
    repo.logout()

    return doc

Regression_Analysis.execute()