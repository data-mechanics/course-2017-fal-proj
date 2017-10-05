import urllib.request
import json
import dml, prov.model
import datetime, uuid
# import geojson

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

City of Boston Open Spaces (Like parks, etc)

Development notes:
Currently accessing a csv, although geoJSON is available - maybe a later alteration depending on use?

"""

class retrieve_open_space(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.open_space']

    @staticmethod
    def execute(trial = False, log=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        # Do retrieving of data
        repo.dropCollection("open_space")
        repo.createCollection("open_space")    
        
        #retaining geoJSON link url for future use
        # url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.csv'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        rowCount = 0
        spaceList = []
        response = response.split('\n')

        fields = response[0].split(',')
        response  = response[1:]
        for line in response:
            lineDict = {}
            items = line.split(',')
            #Some genius put fields with commas inside a csv, so that's the next few lines.. 
            if len(fields) != len(items): 
                    items = items[:9] + [items[9] + items[10]] + items[11:]

            try:
                assert len(fields) == len(items)
            except AssertionError:
                print(line)
                print(items)
                print('\n', fields)
                return

            for i in range(len(items)):                
                lineDict[ fields[i] ] = items[i]
                
            spaceList.append(lineDict)

        repo['bmroach.open_space'].insert_many( spaceList )
        repo['bmroach.open_space'].metadata({'complete':True})  
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

        
                  
        return 





retrieve_open_space.execute()

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
