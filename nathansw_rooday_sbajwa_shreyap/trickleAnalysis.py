import json
import time
import pprint 
import datetime
import dml
import prov.mode
import uuid
import pandas as pd
import numpy as np
import seaborn as sns
from bson import ObjectId
from urllib.request import urlopen
from  itertools import combinations , combinations_with_replacement , product

### Algorithm 2

class trickleAnalysis(dml.Algorithm):

  contributor = 'nathansw_rooday_sbajwa_shreyap'
  ### Make sure this is the correct dataset file name
  reads = ['nathansw_rooday_sbajwa_shreyap.MBTAPerformance', 'nathansw_rooday_sbajwa_shreyap.OTP_by_line']
    
  # Currently it just creates a csv file 
  writes = []

  @staticmethod
  def execute(trial=False):
    startTime = datetime.datetime.now()
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')
    
    mbta_db = repo['nathansw_rooday_sbajwa_shreyap.OTP_by_line']
    mbta_data = mbta_db.find_one()


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



    ## Agents
    this_script = doc.agent('alg:nathansw_rooday_sbajwa_shreyap#trickleAnalysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ## Activities
    get_trickleAnalysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

    ## Entitites
    # Data Source
    resource = 
    # Data Generated
    trickleAnalysis = 
       
    ############################################################

        ## wasAssociatedWith      

    ## used   

    ## wasGeneratedBy

    ## wasAttributedTo    

    ## wasDerivedFrom

    ############################################################

    repo.logout()

    return doc