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

        # Removing unnecessary keys for legibility
        removeEntries = ['country', 'endtime', 'uuid', 'starttime', 'pubmillis', 'turntype']
        x = []
        for data in waze_traffic_json:
            try:
                if data['city'] == 'Boston, MA':
                    for entry in removeEntries:
                        data.pop(entry, None)
                    x.append(data)
            except KeyError:
                pass

        repo.dropCollection("waze_data")
        repo.createCollection("waze_data")
        repo['bkin18_cjoe_klovett_sbrz.waze_data'].insert_many(x)
        repo['bkin18_cjoe_klovett_sbrz.waze_data'].metadata({'complete': True})
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

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        resource = doc.entity('bdp:dih6-az4h.json', {'prov:label': 'Waze Jam Data', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        
        output = doc.entity('dat:bkin18_cjoe_klovett_sbrz#waze_data',
                {prov.model.PROV_LABEL: 'waze_data', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=waze+data&$select=description'
                  }
                  )

        doc.wasAttributedTo(output, this_script)

        doc.wasGeneratedBy(output,this_run, endTime)

        doc.wasDerivedFrom(output, resource, this_run)

        repo.logout()

        return doc
