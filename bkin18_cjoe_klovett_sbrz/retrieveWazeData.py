import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import pdb


class retrieveWazeData(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = []
    writes = ['bkin18_cjoe_klovett_sbrz.waze_data']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')

        # Waze Data Set
        waze_url = urllib.request.Request("https://data.cityofboston.gov/resource/dih6-az4h.json")
        waze_response = urllib.request.urlopen(waze_url).read().decode("utf-8")
        waze_traffic_json = json_util.loads(waze_response)

        #property_assessment_json = property_assessment_json['result']['records']

        repo.dropCollection("waze_data")
        repo.createCollection("waze_data")
        repo['bkin18_cjoe_klovett_sbrz.waze_data'].insert_many(waze_traffic_json)
        repo['bkin18_cjoe_klovett_sbrz.waze_data'].metadata({'complete': True})
        print(repo['bkin18_cjoe_klovett_sbrz.waze_data'].metadata())

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
        doc.add_namespace('cbdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#gatherWazeData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:dih6-az4h.json', {'prov:label': 'Waze Jam Data', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_waze_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_waze_data, this_script)
        doc.usage(get_waze_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=waze+data&$select=description'
                  }
                  )

        waze_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#waze_data',
                {prov.model.PROV_LABEL: 'waze_data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(waze_db, this_script)
        doc.wasGeneratedBy(waze_db,get_waze_data, endTime)
        doc.wasDerivedFrom(waze_db, resource, get_waze_data)

        repo.logout()

        return doc
