from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


class getObesityData(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.ZipCodes']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'http://datamechanics.io/data/biel_otis/zipcodes.json'
        response = urlopen(url).read().decode("utf-8")
        #response = response.replace(' a2,', ',')
        #response = response.replace('[', '')
        #response = '[' + response + ']'

        r = json.loads(response)
        myDict = {}
        myDict['1'] = []
        myList = [myDict]
        for i in r:
            myList[0]['1'].append(i)
        
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("ZipCodes")
        repo.createCollection("ZipCodes")
        repo['biel_otis.ZipCodes'].insert_many(myList)
        repo['biel_otis.ZipCodes'].metadata({'complete':True})
        print(repo['biel_otis.ZipCodes'].metadata())

        """
        url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("found")
        repo.createCollection("found")
        repo['biel_otis.found'].insert_many(r)
        """
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
        repo.authenticate('biel_otis', 'biel_otis')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('zip', 'http://datamechanics.io/biel_otis/') # Dataset containing zipcode information from ZipCodes in Boston

        this_script = doc.agent('alg:biel_otis#getZipCodeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('zip:zipcodes', {'prov:label':'Dataset containing zipcode information from ZipCodes in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_zips = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zips, this_script)
        
        doc.usage(get_zips, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        zips = doc.entity('dat:biel_otis#zipcodes', {prov.model.PROV_LABEL:'Dataset containing zipcode information from ZipCodes in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zips, this_script)
        doc.wasGeneratedBy(zips, get_zips, endTime)
        doc.wasDerivedFrom(zips, resource, get_zips, get_zips, get_zips)
        repo.logout()
        
        return doc

getObesityData.execute()
doc = getObesityData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
