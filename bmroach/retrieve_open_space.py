import urllib.request
import json
import dml, prov.model
import datetime, uuid
import geojson
# import csv

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

City of Boston Open Spaces (Like parks, etc)

Development notes:


"""

class retrieve_open_space(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.open_space']

    @staticmethod
    def execute(trial = False, log=False):
        '''Retrieves open spaces in Boston as geoJSON'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        # Do retrieving of data
        repo.dropCollection("open_space")
        repo.createCollection("open_space")    
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")        
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['bmroach.open_space'].insert_many( geoList )
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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.        
        doc.add_namespace('ops', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:bmroach#open_space', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('ops:2868d370c55d4d458d4ae2224ef8cddd_7', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        
        get_open_space = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_open_space, this_script)
        
        doc.usage(get_open_space,resource, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':''  
                            }
                            )
        
        

        open_space = doc.entity('dat:bmroach#open_space', {prov.model.PROV_LABEL:'open_space', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_space, this_script)
        doc.wasGeneratedBy(open_space, get_open_space, endTime)
        
        doc.wasDerivedFrom(open_space, resource, get_open_space, get_open_space, get_open_space)
      
        repo.logout()                  
        return doc






# retrieve_open_space.execute()
# doc = retrieve_open_space.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
