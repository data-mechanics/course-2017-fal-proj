import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import sys

class retrievePropertyAssessmentData(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = []
    writes = ['bkin18_cjoe_klovett_sbrz.property_assessment']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        print("Retrieving prop assessment...")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')

        # Checks to see if trial is active - only takes 50 sample data points
        TRIAL_NUM = 50 if trial else sys.maxsize
         
        # Property Assessment Data Set
        property_assessment_url = urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?resource_id=062fc6fa-b5ff-4270-86cf-202225e40858&limit=" + str(TRIAL_NUM))
            ## I changed this to 200 because my laptop could not handle 200k datapoints - Chris
        property_assessment_response = urllib.request.urlopen(property_assessment_url).read().decode("utf-8")
        property_assessment_json = json_util.loads(property_assessment_response)
        property_assessment_json = property_assessment_json['result']['records']

        ## Create the collection
        repo.dropCollection("property_assessment")
        repo.createCollection("property_assessment")
        repo['bkin18_cjoe_klovett_sbrz.property_assessment'].insert_many(property_assessment_json)
        repo['bkin18_cjoe_klovett_sbrz.property_assessment'].metadata({'complete': True})
        # print(repo['bkin18_cjoe_klovett_sbrz.property_assessment'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/')

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#retrievePropertyAssessmentData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label': 'Property Assessment FY2017', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_property_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_property_data, this_script)
        doc.usage(get_property_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=property+data&$select=description'
                  }
                  )

        property_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#property_assessment', {prov.model.PROV_LABEL: 'property_assessment', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(property_db, this_script)
        doc.wasGeneratedBy(property_db,get_property_data, endTime)
        doc.wasDerivedFrom(property_db, resource, get_property_data)

        repo.logout()

        return doc
