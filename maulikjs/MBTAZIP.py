import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint
import requests


class MBTAZIP(dml.Algorithm):
    contributor = 'maulikjs'
    reads = ['maulikjs.MBTAstops']
    writes = ['maulikjs.MBTAstopsZIP']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        distMBTA = []
        myDict = {}
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maulikjs', 'maulikjs')
        mbta = repo['maulikjs.MBTAstops']

        for key in mbta.find():
            

            try:
                myDict['stop_id'] = key['stop_id']
                url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng='+str(key['latitude'])+','+str(key['longitude'])+'&result_type=postal_code&key='+dml.auth['services']['Google']['key']
                
                resp = requests.get(url).json()
                zipc = resp['results'][0]['address_components'][0]['long_name']

                myDict['zip']=zipc
                

            except:
                continue

            n=n+1
            if n%1000==0:
                print(n)

            distMBTA.append(myDict.copy())
            



        url = 'https://cs-people.bu.edu/maulikjs/data/zip-persqft-zillow.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("maulikjs.MBTAstopsZIP")
        repo.createCollection("maulikjs.MBTAstopsZIP")
        repo['maulikjs.MBTAstopsZIP'].insert_many(distMBTA)
        repo['maulikjs.MBTAstopsZIP'].metadata({'complete':True})
        print(repo['maulikjs.MBTAstopsZIP'].metadata())

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
        doc.add_namespace('gog','https://maps.googleapis.com/maps/api')

        this_script = doc.agent('alg:MBTAZIP', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:MBTAstops', {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('gog:geocode', {'prov:label':'Google Geocode API', prov.model.PROV_TYPE:'ont:DataResource'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        doc.usage(get_prices, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?latlng=$&result_type=postal_code&key=$'
                  }
                  )
        
        prices = doc.entity('dat:MBTAstopsZIP', {prov.model.PROV_LABEL:'MBTA stops by zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)
        doc.wasDerivedFrom(prices, resource2, get_prices, get_prices, get_prices)

      
        repo.logout()
                  
        return doc

MBTAZIP.execute()
doc = MBTAZIP.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
