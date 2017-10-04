import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class retrieveCollegeUniversityData(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.college_university']
    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston college/university data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        # College/University Data Set
        college_university_url = urllib.request.Request(
            "http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson")
        college_university_response = urllib.request.urlopen(college_university_url).read().decode("utf-8")
        college_university_json = json.loads(college_university_response)['features']

        repo.dropCollection("sbrz_nedg.college_university")
        repo.createCollection("sbrz_nedg.college_university")
        repo['sbrz_nedg.college_university'].insert_many(college_university_json)
        repo['sbrz_nedg.college_university'].metadata({'complete':True})
        print(repo['sbrz_nedg.college_university'].metadata())

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
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', '"http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:sbrz_nedg#retrieveCollegeUniversityData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:cbf14bb032ef4bd38e20429f71acb61a_2', {'prov:label': 'Colleges and Universities', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        get_college_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(retrieveCollegeUniversityData, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': ''}) # There is no query

        college_db = doc.entity('dat:sbrz_nedg#get_college_data', {prov.model.PROV_LABEL: 'college_university', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(this_script)
        doc.wasGeneratedBy(get_college_data)
        doc.wasDerivedFrom(college_db, resource)

        repo.logout()

        return doc