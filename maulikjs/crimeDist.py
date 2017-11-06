import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint

class crimeDist(dml.Algorithm):
    contributor = 'maulikjs'
    reads = ['maulikjs.crimes','maulikjs.DistPolice']
    writes = ['maulikjs.CrimeDist']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        crimeDist = []
        myDict = {}
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maulikjs', 'maulikjs')
        crimes = repo['maulikjs.crimes']
        dist = repo['maulikjs.DistPolice']
        # pprint.pprint(prices.find_one({"RegionName":"02134"}))
        Districts= []
        for key in dist.find():
            Districts.append(key['District'].replace('-',''))

        for x in Districts:
            n=0
            for key in crimes.find({"DISTRICT":x}):
                n=n+1

            myDict['District']=x
            myDict['Count']=n


            crimeDist.append(myDict.copy())


        repo.dropCollection("maulikjs.crimeDist")
        repo.createCollection("maulikjs.crimeDist")
        repo['maulikjs.crimeDist'].insert_many(crimeDist)
        repo['maulikjs.crimeDist'].metadata({'complete':True})
        print(repo['maulikjs.crimeDist'].metadata())


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

        this_script = doc.agent('alg:crimeDist', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:crimes', {'prov:label':'Crimes in Boston', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('dat:DistPolice', {'prov:label':'Police Station Districts', prov.model.PROV_TYPE:'ont:DataResource'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        doc.usage(get_prices, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        prices = doc.entity('dat:CrimeDist', {prov.model.PROV_LABEL:'Crimes Per Police District', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)
        doc.wasDerivedFrom(prices, resource2, get_prices, get_prices, get_prices)


      
        repo.logout()
                  
        return doc

crimeDist.execute()
doc = crimeDist.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

#eof
