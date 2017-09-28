import urllib.request
import json
import pandas as pd
import dml
import prov.model
import datetime
import uuid
import sys
from bson import json_util

class build_shooting_set(dml.Algorithm):
    contributor = 'esaracin'
    reads = ['esaracin.boston_shootings']
    writes = ['esaracin.shootings_per_district']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')


        # Load in our repository datasets as Pandas DataFrames.
        dataset = repo['esaracin.boston_shootings'].find()
        df_shootings = pd.DataFrame(list(dataset))


        crime_types = df_shootings['INCIDENT_TYPE_DESCRIPTION'].unique()
        police_districts = df_shootings['REPTDISTRICT'].unique()


        # Initialize our empty data table using a kind of Cartesian Product.
        data = {'District': police_districts}
        to_append = {crime: [0 for x in police_districts] for crime in crime_types}
        data.update(to_append)

        
        # Walk through our data, incrementing those positions in the table
        # necessary. Use a nested structure to find the correct index to
        # increment each shooting at.
        for index, row, in df_shootings.iterrows():
            index = 0
            for district in police_districts:
                if(row['REPTDISTRICT'] == district):
                    data[row['INCIDENT_TYPE_DESCRIPTION']][index] += 1
                    break

                index += 1

        

        # Now, we can put our new dataset into MongoDB by converting it to
        # JSON and inserting it.

        final_df = pd.DataFrame(data)
        json_table = final_df.to_json(orient='records')
        r = json.loads(json_table)

        repo.dropCollection("shootings_per_district")
        repo.createCollection("shootings_per_district")
        repo['esaracin.shootings_per_district'].insert_many(r)
        repo['esaracin.shootings_per_district'].metadata({'complete':True})
        print(repo['esaracin.shootings_per_district'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 


build_shooting_set.execute()
