import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getColleges(dml.Algorithm):
    contributor = 'jdbrawn_slarbi'
    reads = []
    writes = ['jdbrawn_slarbi.colleges']

    @staticmethod
    def execute(trial = False):
        """Retrieve the college location data from Analyze Boston"""
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        url = 'https://data.boston.gov/datastore/odata3.0/208dc980-a278-49e3-b95b-e193bb7bb6e4?$top=65&$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("colleges")
        repo.createCollection("colleges")
        repo['jdbrawn_slarbi.colleges'].insert_many(r)

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
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/datastore/')

        this_script = doc.agent('alg:jdbrawn_slarbi#getColleges',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('208dc980-a278-49e3-b95b-e193bb7bb6e4',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_colleges = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_colleges, this_script)
        doc.usage(get_colleges, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        colleges = doc.entity('dat:jdbrawn_slarbi#crime',
                           {prov.model.PROV_LABEL: 'Boston Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(colleges, this_script)
        doc.wasGeneratedBy(colleges, get_colleges, endTime)
        doc.wasDerivedFrom(colleges, resource, get_colleges, get_colleges, get_colleges)

        repo.logout()

        return doc
