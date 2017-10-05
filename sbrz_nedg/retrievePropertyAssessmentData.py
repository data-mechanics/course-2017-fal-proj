import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid


class retrievePropertyAssessmentData(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.property_assessment']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        # Property Assessment Data Set
        property_assessment_url = urllib.request.Request(
            "https://data.boston.gov/api/action/datastore_search?resource_id=062fc6fa-b5ff-4270-86cf-202225e40858&limit=200000")
        property_assessment_response = urllib.request.urlopen(property_assessment_url).read().decode("utf-8")
        property_assessment_json = json_util.loads(property_assessment_response)

        property_assessment_json = property_assessment_json['result']['records']

        repo.dropCollection("property_assessment")
        repo.createCollection("property_assessment")
        repo['sbrz_nedg.property_assessment'].insert_many(property_assessment_json)
        repo['sbrz_nedg.property_assessment'].metadata({'complete': True})
        print(repo['sbrz_nedg.property_assessment'].metadata())

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
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/')

        this_script = doc.agent('alg:sbrz_nedg#retrievePropertyAssessmentData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label': 'Property Assessment FY2017', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_property_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': '&limit=200000'})

        property_db = doc.entity('dat:sbrz_nedg#property_assessment', {prov.model.PROV_LABEL: 'property_assessment', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(this_script, this_script)
        doc.wasGeneratedBy(get_property_data)
        doc.wasDerivedFrom(resource)

        repo.logout()

        return doc
