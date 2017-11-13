import urllib.request
import json
import pandas as pd
import numpy as np
import dml
import prov.model
import datetime
import uuid
import sys
from bson import json_util

# For linear regression
import statsmodels.api as sm

class race_linear_analysis(dml.Algorithm):
    contributor = 'esaracin'
    reads = ['esaracin.crime_incidents', 'esaracin.fio_data']
    writes = ['esaracin.race_data']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')


        # Support for Trial mode:       
        if(trial == True):
            # Skip all but 1 percent of each collection
            dataset = repo['esaracin.crime_incidents'].find().skip(repo['esaracin.crime_incidents'].count()- 2393)
            datasetFIO = repo['esaracin.fio_data'].find().skip(repo['esaracin.crime_incidents'].count() - 1522)
        else:

            dataset = repo['esaracin.crime_incidents'].find()
            datasetFIO = repo['esaracin.fio_data'].find()

        df_crime = pd.DataFrame(list(dataset))
        df_fios = pd.DataFrame(list(datasetFIO))
       
        url = 'http://datamechanics.io/data/district_racial_composition.csv'
        df_race = pd.read_csv(url)


        # Now we need to find the number of crimes/fios for each Policing
        # District.
        crime_by_district = {dist: 0 for dist in df_crime['DISTRICT'].unique()}
        del crime_by_district['A15']
        del crime_by_district['A1']
        crime_by_district['A1/A15'] = 0

        for index, row in df_crime.iterrows():
            if row['DISTRICT'] == 'A1' or row['DISTRICT'] == 'A15':
                crime_by_district['A1/A15'] += 1
            else:
                crime_by_district[row['DISTRICT']] += 1

        
        # Replace former crime DataFrame with this new, filtered, data.
        # Use it to join df_race on the District field.
        df_crime = pd.DataFrame.from_dict(crime_by_district, orient='index')
        df_race = df_race.join(df_crime, on='dist')
        new_columns = df_race.columns.values
        new_columns[-1] = 'Crime Count'
        df_race.columns = new_columns


        # Similarly compute the number of FIOs in each District.
        # Join this with our growing df_race table as well.
        fios_by_district = {dist:0 for dist in df_race['dist']}
        for index, row in df_fios.iterrows():
            if(row['DIST'] in fios_by_district):
                fios_by_district[row['DIST']] += 1
            elif(row['DIST'] == 'A1' or row['DIST'] == 'A15'):
                fios_by_district['A1/A15'] +=1

        df_fios = pd.DataFrame.from_dict(fios_by_district, orient='index')
        df_race = df_race.join(df_fios, on='dist')
        new_columns = df_race.columns.values
        new_columns[-1] = 'FIO Count'
        df_race.columns = new_columns


        # Normalize crime count and FIO count by population of district.
        for index, row in df_race.iterrows():
            df_race.ix[index, 'Crime Count'] /= row['population']
            df_race.ix[index, 'FIO Count'] /= row['population']


        # Now drop the categorical data before the regression
        to_insert = df_race.to_json(orient='records') # Save to insert later

        districts = df_race['dist']
        df_race = df_race.drop('dist', axis=1).drop('dist_name', axis=1)
        df_race = df_race.drop('population', axis=1)
 
        # Run regression with the number of crimes in each area as the output
        # attribute.
        y_train = df_race['FIO Count']
        X_train = df_race.drop('FIO Count', axis=1)
                                     
        model = sm.OLS(y_train, X_train)
        results = model.fit()

        outfile = open('Linear_Reg_Results.txt', 'w')
        print(results.summary(), file=outfile)
        outfile.close()
       


        # Insert our race dataset.
        r = json.loads(to_insert)
        
        repo.dropCollection("race_data")
        repo.createCollection("race_data")
        repo['esaracin.race_data'].insert_many(r)
        repo['esaracin.race_data'].metadata({'complete':True})
        print(repo['esaracin.race_data'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        '''Creates the provenance document describing the merging of data
        occuring within this script.'''
         
        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Add useful namespaces for this prov doc
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics/io/ontology/')
        doc.add_namespace('log', 'http://datamechanics.io/log/')

        # Add this script as a provenance agent to our document. Also add the
        # entity and activity utilized and completed by this script.
        this_script = doc.agent('alg:esaracin#race_linear_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crimes = doc.entity('dat:esaracin#crime_incidents',{'prov:label':'MongoDB Set',prov.model.PROV_TYPE:'ont:DataResource'})
        resource_fios = doc.entity('dat:esaracin#fio_data',{'prov:label':'MongoDB Set',prov.model.PROV_TYPE:'ont:DataResource'})
        resource_race = doc.entity('dat:esaracin#fio_data',{'prov:label':'MongoDB Set',prov.model.PROV_TYPE:'ont:DataResource'})
        
        
        regression = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_race = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(regression, this_script)
        doc.usage(regression, resource_crimes, startTime, None, {prov.model.PROV_TYPE:'ont:Transformation'})
        doc.usage(regression, resource_fios, startTime, None, {prov.model.PROV_TYPE:'ont:Transformation'})
        doc.usage(regression, resource_race, startTime, None, {prov.model.PROV_TYPE:'ont:Transformation'})

        doc.wasAssociatedWith(get_race, this_script)
        doc.usage(get_race, resource_race)

        race_data = doc.entity('dat:esaracin#race_data', {prov.model.PROV_LABEL:'Race Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(race_data, this_script)
        doc.wasGeneratedBy(race_data, get_race)
        doc.wasDerivedFrom(race_data, resource_race, get_race, get_race, get_race)


        repo.logout()
        return doc


