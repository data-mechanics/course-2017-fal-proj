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

        # Add this script as a provenance agent to our document
        this_script = doc.agent('alg:esaracin#merge_police_sets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_districts = doc.entity('dat:esaracin#police_districts',{'prov:label':'MongoDB Set',prov.model.PROV_TYPE:'ont:DataResource'})
        resource_crime = doc.entity('dat:esaracin#crime_incidents', {'prov:label': 'MongoDB Set', prov.model.PROV_TYPE:'ont:DataResource'})
        
        merge_sets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(merge_sets, this_script)
        doc.usage(merge_sets, resource_districts, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(merge_sets, resource_crime, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})


        merged = doc.entity('dat:esaracin#police_stats', {prov.model.PROV_LABEL:'Merged Set',
                            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(merged, this_script)
        doc.wasGeneratedBy(merged, merge_sets, endTime)
        doc.wasDerivedFrom(merged, resource_districts, merge_sets, merge_sets,
                          merge_sets)
        doc.wasDerivedFrom(merged, resource_crime, merge_sets, merge_sets,
                           merge_sets)


        repo.logout()
        return doc


