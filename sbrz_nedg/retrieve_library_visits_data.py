import urllib.request
from bson import json_util
import prov.model
import dml
import datetime
import uuid



class example(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.library_visits']
    @staticmethod
    def execute(trial = False):
        '''Retrieve library_visits data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        library_visits_url = urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        # library visit Data Set.
        #library_visits_url = urllib.request.Request(
            #"https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")
        #library_visits_response = urllib.request.urlopen(urllib.request.Request(
            #"https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8")
        library_visits_json = json_util.loads(urllib.request.urlopen(urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?resource_id=0d81febc-c7f8-4de3-b8f4-a18733b1c11b")).read().decode("utf-8"))

        #library_visits_json = library_visits_json

        #repo.dropCollection("library_visits")
        #repo.createCollection("library_visits")
        #repo['sbrz_nedg.library_visits'].insert_many(library_visits_json)
        #repo['sbrz_nedg.library_visits'].metadata({'complete':True})
        #print(repo['sbrz_nedg.library_visits'].metadata())
        print(library_visits_json)

        repo.logout()

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
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:sbrz_nedg#example',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_found = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        lost = doc.entity('dat:sbrz_nedg#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:sbrz_nedg#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()

        return doc
example.execute()