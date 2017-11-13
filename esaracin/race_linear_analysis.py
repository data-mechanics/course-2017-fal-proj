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
    writes = []

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('esaracin', 'esaracin')

        # Grab all of our two databases that we're reading from. Also read from
        # a csv our third dataset
        dataset = repo['esaracin.crime_incidents'].find()
        df_crime = pd.DataFrame(list(dataset))
       
        dataset = repo['esaracin.fio_data'].find()
        df_fios = pd.DataFrame(list(dataset))
       
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
       

        # JSON to insert
#        json_set = df_race.to_json(orient='records')
#        r = json.loads(json_set)

        
 #       repo.dropCollection("crime_incident_centers")
 #       repo.createCollection("crime_incident_centers")
 #       repo['esaracin.crime_incident_centers'].insert_many(r)
 #       repo['esaracin.crime_incident_centers'].metadata({'complete':True})
 #       print(repo['esaracin.crime_incident_centers'].metadata())


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
        this_script = doc.agent('alg:esaracin#kmeans_crime_incidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crimes = doc.entity('dat:esaracin#crime_incidents',{'prov:label':'MongoDB Set',prov.model.PROV_TYPE:'ont:DataResource'})
        clustering = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(clustering, this_script)
        doc.usage(clustering, resource_crimes, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        clustered = doc.entity('dat:esaracin#crime_incident_centers', {prov.model.PROV_LABEL:'Clustered Set', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clustered, this_script)
        doc.wasGeneratedBy(clustered, clustering, endTime)
        doc.wasDerivedFrom(clustered, resource_crimes, clustering, clustering, clustering)


        repo.logout()
        return doc

race_linear_analysis.execute()
