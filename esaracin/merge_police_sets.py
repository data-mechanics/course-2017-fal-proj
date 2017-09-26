import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid
import sys
from bson import json_util

class merge_police_sets(dml.Algorithm):
    contributor = 'esaracin'
    reads = ['esaracin.police_districts', 'esaracin.crime_incidents']
    writes = ['esaracin.police_stats']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Grab all of our two databases that we're reading from.
        dataset = repo['esaracin.police_districts'].find()
        df_districts = pd.DataFrame(list(dataset))

        dataset = repo['esaracin.crime_incidents'].find()
        df_crime = pd.DataFrame(list(dataset))

    
        # Now, apply an aggregate to our dataset through the count()
        # function. This will return the number of crimes for each policing 
        # district.
        df_crime = df_crime.groupby('DISTRICT').count()['_id']


        # Finally, we can join the two datasets on the DISTRICT key to create
        # our new, combined dataset.
        new_df = df_districts.join(df_crime, on='DISTRICT', rsuffix='_crime_count').reset_index()

        # While we're add it, lets clean our data by projecting away those
        # attributes that are duplicates/uesless.
        new_df = new_df.drop('DISTRICT_', axis=1)
        new_df = new_df.drop('DISTRICT__', axis=1)
        new_df = new_df.drop('_id', axis=1)
        new_df = new_df.drop('ID', axis=1)
 

        # Now, we can put our dataset back into MongoDB by converting it to
        # JSON and reinserting it.
        json_set = new_df.to_json(orient='records')
        r = json.loads(json_set)

        repo.dropCollection("police_stats")
        repo.createCollection("police_stats")
        repo['esaracin.police_stats'].insert_many(r)
        repo['esaracin.police_stats'].metadata({'complete':True})
        print(repo['esaracin.police_stats'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 


merge_police_sets.execute()
