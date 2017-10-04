import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getCrimeData(dml.Algorithm):
    contributor = 'rooday_shreyapandit'
    reads = []
    writes = ['rooday_shreyapandit.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        #Get crime data
        # url = 'https://data.boston.gov/api/action/datastore_search?limit=5&q=title:jones'
        url_crime = "https://data.boston.gov/api/action/datastore_search_sql?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22"
        #'http://localhost:8890/crime.json'
        response_crime = requests.get(url_crime).json()
        print("crime response has come, inserting....")
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['rooday_shreyapandit.crime'].insert_many(response_crime['result']['records'])
        repo['rooday_shreyapandit.crime'].metadata({'complete':True})
        print("crime response has been inserted")
        print(repo['rooday_shreyapandit.crime'].metadata())
        repo.logout()


        endTime = datetime.datetime.now()
        print("Done!")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/rooday_shreyapandit') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/rooday_shreyapandit') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('crime', 'https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system/resource/')

        this_script = doc.agent('alg:#getCrimeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('crime:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime_data, this_script)
        doc.usage(get_crime_data, resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval'})
        crime = doc.entity('dat:#crime', {prov.model.PROV_LABEL:'Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime_data, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime_data, get_crime_data, get_crime_data)

        repo.logout()
                  
        return doc

# getCrimeData.execute()
# print("running provenance for getCrimeData")
# doc = getCrimeData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))