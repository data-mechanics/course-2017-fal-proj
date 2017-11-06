import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint

class DistPolice(dml.Algorithm):
    contributor = 'maulikjs'
    reads = ['maulikjs.police']
    writes = ['maulikjs.DistPolice']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        distPolice = []
        myDict = {}
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maulikjs', 'maulikjs')
        police = repo['maulikjs.police']

        # pprint.pprint(prices.find_one({"RegionName":"02134"}))

        for key in police.find():
            myDict['District'] = key['NAME'].split(' ')[1]
            if myDict['District']== 'Police':
                myDict['District'] = 'HQ'

            myDict['Zip']= key['ZIP']
            myDict['longitude'] = key['longitude']
            myDict['latitude']= key['latitude']
            distPolice.append(myDict.copy())



        # url = 'https://cs-people.bu.edu/maulikjs/data/zip-persqft-zillow.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("maulikjs.DistPolice")
        repo.createCollection("maulikjs.DistPolice")
        repo['maulikjs.DistPolice'].insert_many(distPolice)
        repo['maulikjs.DistPolice'].metadata({'complete':True})
        print(repo['maulikjs.DistPolice'].metadata())

        # while True:

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
        repo.authenticate('maulikjs', 'maulikjs')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/maulikjs#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/maulikjs#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:DistPolice', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:police', {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        
        prices = doc.entity('dat:DistPolice', {prov.model.PROV_LABEL:'Getting Police District', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)

      
        repo.logout()
                  
        return doc

DistPolice.execute()
doc = DistPolice.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
