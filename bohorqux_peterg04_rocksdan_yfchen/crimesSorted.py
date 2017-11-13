import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimesSorted(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.crimes']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.sorted']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        crimes = {}
        stats = []

        reports = repo['bohorqux_peterg04_rocksdan_yfchen.crimes']

        for r in reports.find():
            if r["OFFENSE_CODE_GROUP"] not in crimes:
                crimes[r["OFFENSE_CODE_GROUP"]] = 0
            else:
                crimes[r["OFFENSE_CODE_GROUP"]] += 1

        for offense in crimes:
            stats.append({"Offense":offense, "Count":crimes[offense]})

        repo.dropCollection("sorted")
        repo.createCollection("sorted")
        repo['bohorqux_peterg04_rocksdan_yfchen.sorted'].insert_many(stats)
        repo['bohorqux_peterg04_rocksdan_yfchen.sorted'].metadata({'complete':True})
        print(repo['bohorqux_rocksdan.sorted'].metadata())

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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#crimesSorted', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crimes, this_script)
        doc.usage(get_crimes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?OFFENSE_CODE_GROUP=Residential+Burglary&$select=OFFENSE_CODE_GROUP'
                  }
                  )
        crimes = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#sorted', {prov.model.PROV_LABEL:'Crime Report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get_crimes, endTime)
        doc.wasDerivedFrom(crimes, resource, get_crimes, get_crimes, get_crimes)

        repo.logout()
                  
        return doc

crimesSorted.execute()
doc = crimesSorted.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
