import urllib.request
import json
import dml, prov.model
import datetime, uuid
import geojson

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

City of Boston existing Bike Network

Development notes:
cvs alt url
url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.csv'

"""

class retrieve_bike_network(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.bike_network']

    @staticmethod
    def execute(trial = False, log=False):
        '''Retrieve existing bike network data.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        # Do retrieving of data
        repo.dropCollection("bike_network")
        repo.createCollection("bike_network")    
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        
        repo['bmroach.bike_network'].insert_many( geoList )
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
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.        
        doc.add_namespace('bik', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:bmroach#retrieve_bike_network', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bik:d02c9d2003af455fbc37f550cc53d3a4_0', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        
        get_bike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_bike, this_script)
        
        doc.usage(get_bike,resource, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':''  
                            }
                            )
        
        bike_network = doc.entity('dat:bmroach#bike_network', {prov.model.PROV_LABEL:'bike_network', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bike_network, this_script)
        doc.wasGeneratedBy(bike_network, get_bike, endTime)
        
        doc.wasDerivedFrom(bike_network, resource, get_bike, get_bike, get_bike)
      
        repo.logout()                  
        return doc


# retrieve_bike_network.execute()
# doc = retrieve_bike_network.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
