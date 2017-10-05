import urllib.request
from bson import json_util
import prov.model
import dml
import datetime
import uuid



class retrieve_library_visits_data(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.libraryData']
    @staticmethod
    def execute(trial = False):
        '''Retrieve library_visits data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        #library_visits_url = urllib.request.Request(
            #"https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        #"https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b"
        # library visit Data Set.
        library_visits_json_one = json_util.loads(urllib.request.urlopen(urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8"))
        library_visits_json_two = json_util.loads(urllib.request.urlopen(urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?offset=100&resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8"))
        library_visits_json_three = json_util.loads(urllib.request.urlopen(urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?offset=200&resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8"))
        library_visits_json_four = json_util.loads(urllib.request.urlopen(urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?offset=300&resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8"))
        library_visits_json_five = json_util.loads(urllib.request.urlopen(urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?offset=400&resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8"))
        repo.dropCollection("libraryData")
        repo.createCollection("libraryData")
        print(library_visits_json_one['result'])
        repo['sbrz_nedg.libraryData'].insert_one(library_visits_json_one)
        print(library_visits_json_two['result'])
        repo['sbrz_nedg.libraryData'].insert_one(library_visits_json_two)
        print(library_visits_json_three['result'])
        repo['sbrz_nedg.libraryData'].insert_one(library_visits_json_three)
        print(library_visits_json_four['result'])
        repo['sbrz_nedg.libraryData'].insert_one(library_visits_json_four)
        print(library_visits_json_five['result'])
        repo['sbrz_nedg.libraryData'].insert_one(library_visits_json_five)

        repo.logout

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/')


        this_script = doc.agent('alg:sbrz_nedg#retrieve_Library_Visits_Data', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:0d81febc-c7f8-4de3-b8f4-a18733b1c11b', {'prov:label': 'Library Visits 2014-2016', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_library_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(retrieve_library_visits_data, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': '&limit=200000'})

        library_db = doc.entity('dat:sbrz_nedg#get_library_data', {prov.model.PROV_LABEL: 'library_visits', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(this_script)
        doc.wasGeneratedBy(get_library_data)
        doc.wasDerivedFrom(library_db, resource)


        repo.logout()

        return doc
retrieve_library_visits_data.execute()