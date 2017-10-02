import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = []
    writes = ['sbrz_nedg.lost', 'sbrz_nedg.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        """
        # Property Assessment Data Set
        property_assessment_url = urllib.request.Request(
            "https://data.boston.gov/export/062/fc6/062fc6fa-b5ff-4270-86cf-202225e40858.json")
        property_assessment_response = urllib.request.urlopen(property_assessment_url).read().decode("utf-8")
        property_assessment_json = json.loads(property_assessment_response)

        print(property_assessment_json)

        # Alcohol Licences Data Set
        alcohol_license_url = urllib.request.Request(
            "https://data.boston.gov/export/9e1/5f4/9e15f457-1923-4c12-9992-43ba2f0dd5e5.json")
        alcohol_license_response = urllib.request.urlopen(alcohol_license_url).read().decode("utf-8")
        alcohol_license_json = json.loads(alcohol_license_response)
        """
        # Boston Colleges & Universities Data Set
        colleges_and_univ_url = urllib.request.Request(
            "http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson")
        colleges_and_univ_response = urllib.request.urlopen(colleges_and_univ_url).read().decode("utf-8")
        colleges_and_univ_json = json.loads(colleges_and_univ_response)

        print(colleges_and_univ_json)
        """
        # Boston 311 Service Requests Data Set
        three_one_one_req_url = urllib.request.Request(
            "https://data.boston.gov/export/296/8e2/2968e2c0-d479-49ba-a884-4ef523ada3c0.json")
        three_one_one_req_response = urllib.request.urlopen(three_one_one_req_url).read().decode("utf-8")
        three_one_one_req_json = json.loads(three_one_one_req_response)

        # Boston Fire Incident Data Set
        fire_incident_url = three_one_one_req_url = urllib.request.Request(
            "https://data.boston.gov/dataset/fire-incident-reporting/resource/f9a21363-aff6-4840-a2d0-c738cb1c30a1")
        fire_incident_response = urllib.request.urlopen(fire_incident_url).read().decode("utf-8")
        fire_incident_json = json.loads(fire_incident_response)
        """
        url = 'http://cs-people.bu.edu/lapets/591/examples/lost.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("lost")
        repo.createCollection("lost")
        repo['sbrz_nedg.lost'].insert_many(r)
        repo['sbrz_nedg.lost'].metadata({'complete':True})
        print(repo['sbrz_nedg.lost'].metadata())

        url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("found")
        repo.createCollection("found")
        repo['sbrz_nedg.found'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

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
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:sbrz_nedg#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:sbrz_nedg#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:sbrz_nedg#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


