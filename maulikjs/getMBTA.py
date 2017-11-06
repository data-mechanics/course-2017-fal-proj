import json
import dml
import prov.model
import datetime
import uuid
import zipfile 
import io
from urllib import parse, request
from json import loads, dumps, load


class getMBTA(dml.Algorithm):
    contributor = 'maulikjs'
    reads = []
    writes = ['maulikjs.MBTAstops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maulikjs', 'maulikjs')

        url = 'https://www.mbta.com/uploadedfiles/MBTA_GTFS.zip'
        
        with request.urlopen(url) as resp:
            response = resp.read()

            with zipfile.ZipFile(io.BytesIO(response)) as z:
                for file in z.namelist():
                    if file in ['stops.txt']:
                        with z.open(file,'r') as file2:
                            mbtafile = file2.readlines()

                            mbta = []

                            for stop in mbtafile[1:]:

                                allstr = stop.decode("utf-8").replace('"', "").split(",")

                                stop_id = allstr[0]
                                stop_name = allstr[2]

                                try:
                                    lat= float(allstr[4])
                                except ValueError:
                                    continue

                                try:
                                    lon=float(allstr[5])

                                except ValueError:
                                    continue

                            

                                mbta.append({"stop_id": stop_id,"stop_name": stop_name, "latitude":lat, "longitude":lon})

                            repo.dropCollection("MBTAstops")
                            repo.createCollection("MBTAstops")

                            repo['maulikjs.MBTAstops'].insert_many(mbta)
                            repo['maulikjs.MBTAstops'].metadata({'complete':True})

                            print(repo['maulikjs.MBTAstops'].metadata())




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
        doc.add_namespace('mbt', 'https://www.mbta.com/uploadedfiles/')

        this_script = doc.agent('alg:getMBTA', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbt:MBTA_GTFS.zip', {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        
        prices = doc.entity('dat:MBTAstops', {prov.model.PROV_LABEL:'MBTAstops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)

      
        repo.logout()
                  
        return doc

getMBTA.execute()
doc = getMBTA.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
