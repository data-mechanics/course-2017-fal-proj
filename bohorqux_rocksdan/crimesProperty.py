import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimesProperty(dml.Algorithm):
    contributor = 'bohorqux_rocksdan'
    reads = ['bohorqux_rocksdan.properties', 'bohorqux_rocksdan.crimes']
    writes = ['bohorqux_rocksdan.property_crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_rocksdan', 'bohorqux_rocksdan')

        properties = repo['bohorqux_rocksdan.properties']
        crimes = repo['bohorqux_rocksdan.crimes']
        p = []
        c = []
        union = []

        for houses in properties.find():
            p.append({"Street":houses["ST_NAME"], "Cost":houses["AV_TOTAL"], "Gross Tax":houses["GROSS_TAX"]})

        for reports in crimes.find():
            c.append({"Offense":reports["OFFENSE_CODE_GROUP"], "Desc":reports["OFFENSE_DESCRIPTION"], "Street":reports["STREET"]})

        for i in range(100):
            union.append({"Street": p[i]["Street"], "Cost": p[i]["Cost"], "Gross Tax": p[i]["Gross Tax"], "Offense":c[i]["Offense"], "Desc":c[i]["Desc"]})
            
                
        repo.dropCollection("property_crimes")
        repo.createCollection("property_crimes")
        repo['bohorqux_rocksdan.property_crimes'].insert_many(union)
        repo['bohorqux_rocksdan.property_crimes'].metadata({'complete':True})
        print(repo['bohorqux_rocksdan.property_crimes'].metadata())

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
        repo.authenticate('bohorqux_rocksdan', 'bohorqux_rocksdan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bohorqux_rocksdan#crimesProperty', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_properties = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_properties, this_script)
        doc.usage(get_properties, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?Street=TREMONT ST&$select=Street'
                  }
                  )
        properties = doc.entity('dat:bohorqux_rocksdan#property_crimes', {prov.model.PROV_LABEL:'Property Values', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties, this_script)
        doc.wasGeneratedBy(properties, get_properties, endTime)
        doc.wasDerivedFrom(properties, resource, get_properties, get_properties, get_properties)

        repo.logout()
                  
        return doc

crimesProperty.execute()
doc = crimesProperty.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
