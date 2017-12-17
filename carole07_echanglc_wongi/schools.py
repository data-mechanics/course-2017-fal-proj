import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class schools(dml.Algorithm):
    contributor = 'carole07_echanglc_wongi'
    reads = []
    writes = ['carole07_echanglc_wongi.schools']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Public Schools Data from City of Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        url = 'https://boston.opendatasoft.com/api/records/1.0/search/?dataset=public-schools&rows=-1'

        response = ("[" + urllib.request.urlopen(url).read() + "]").decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['carole07_echanglc_wongi.schools'].insert(r["records"])
        print('Load public schools')
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def execute(trial = True):
        '''Retrieve Boston Public Schools Data from City of Boston .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
        
        url = 'https://boston.opendatasoft.com/api/records/1.0/search/?dataset=public-schools&rows=-1'
        
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['carole07_echanglc_wongi.schools'].insert(r["records"])
        print('Load public schools')
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
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:carole07_echanglc_wongi#schools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:1d9509a8b2fd485d9ad471ba2fdb1f90_0', {'prov:label':'Boston Public School', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        schools = doc.entity('dat:carole07_echanglc_wongi#schools', {prov.model.PROV_LABEL:'Boston Public Schools ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource, get_schools, get_schools, get_schools)

        repo.logout()

        return doc

