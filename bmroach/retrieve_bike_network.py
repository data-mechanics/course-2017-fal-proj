import urllib.request
import json
import dml, prov.model
import datetime, uuid
# import geojson

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

City of Boston existing Bike Network

Development notes:
Currently accessing a csv, although geoJSON is available - maybe a later alteration depending on use?

"""

class retrieve_bike_network(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.bike_network']

    @staticmethod
    def execute(trial = False, log=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        # Do retrieving of data
        repo.dropCollection("bike_network")
        repo.createCollection("bike_network")    
        
        #Saving in case of future geoJSON use
        # url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.csv'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        rowCount = 0
        netList = []
        response = response.split('\n')


        fields = response[0].split(',')
        response  = response[1:]
        for line in response:
            lineDict = {}
            items = line.split(',')

            for i in range(len(items)):
                lineDict[ fields[i] ] = items[i]
            netList.append(lineDict)

        repo['bmroach.bike_network'].insert_many( netList )
        repo['bmroach.bike_network'].metadata({'complete':True})  
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





retrieve_bike_network.execute()

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
