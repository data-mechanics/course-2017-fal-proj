import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid
import sys
from bson import json_util

class join_sets(dml.Algorithm):
    contributor = 'esaracin'
    reads = ['esaracin.shootings_per_district', 'esaracin.police_stats']
    writes = ['esaracin.district_info']

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
        dataset = repo['esaracin.police_stats'].find()
        df_police = pd.DataFrame(list(dataset))

        dataset = repo['esaracin.shootings_per_district'].find()
        df_shootings = pd.DataFrame(list(dataset))

        # Rename our district column to make the join easier
        df_shootings = df_shootings.rename(columns = {'District':'DISTRICT'}).drop('_id',axis=1)


        # Finally, we can join the two datasets on the DISTRICT key to create
        # our new, combined dataset.
        new_df = df_police.join(df_shootings.set_index('DISTRICT'), on='DISTRICT').reset_index()


        # While we're add it, lets clean our data by projecting away those
        # attributes that are duplicates/uesless.
        new_df = new_df.drop('_id', axis=1)


        # Iterate through our data, compiling a percentage of the shootings
        # compared to the overall number of crimes. Projects this new
        # information as part of each tuple
        perc_shootings = []
        for index, row in new_df.iterrows():
            shooting_count = sum(row[8:])
            crime_count = row['_id_crime_count']

            perc_shootings.append(shooting_count / crime_count)

        new_df['shooting_crime_percentage'] = pd.Series(perc_shootings)


        # Now, we can put our dataset back into MongoDB by converting it to
        # JSON and reinserting it.
        json_set = new_df.to_json(orient='records')
        r = json.loads(json_set)

        repo.dropCollection("district_info")
        repo.createCollection("district_info")
        repo['esaracin.district_info'].insert_many(r)
        repo['esaracin.district_info'].metadata({'complete':True})
        print(repo['esaracin.district_info'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 
join_sets.execute()
