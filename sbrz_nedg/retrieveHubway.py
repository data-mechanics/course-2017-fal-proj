import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid


class retrieveHubway(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.hubway']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        # Property Assessment Data Set
        hubway_url = urllib.request.Request(
            "http://bostonopendata-boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_0.geojson")
        hubway_response = urllib.request.urlopen(hubway_url).read().decode("utf-8")
        hubway_json = json_util.loads(hubway_response)['features']

        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        repo['sbrz_nedg.hubway'].insert_many(hubway_json)
        repo['sbrz_nedg.hubway'].metadata({'complete': True})
        print(repo['sbrz_nedg.hubway'].metadata())

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
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:sbrz_nedg#retrieveHubway', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:ee7474e2a0aa45cbbdfe0b747a5eb032_0', {'prov:label': 'Property Assessment FY2017', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        get_hubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': ''})# There is no query used in retrieval

        property_db = doc.entity('dat:sbrz_nedg#get_hubway', {prov.model.PROV_LABEL: 'hubway', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(this_script, this_script)
        doc.wasGeneratedBy(get_hubway)
        doc.wasDerivedFrom(property_db, resource)

        repo.logout()

        return doc
