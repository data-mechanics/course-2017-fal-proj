import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class metersneighbors(dml.Algorithm):
    contributor = 'medinad'
    reads = ['medinad.meters','medinad.neighborhoods']
    writes = ['medinad.meters-neighborhoods']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')

        

        meters1 = repo.medinad.meters
        neighborhoods = repo.medinad.neighborhoods
        

        new_meters = []

        new_meters = [{'Meters Point':x["fields"]["geo_point_2d"]} for x in meters1]


        new_neighborhoods = [{'Neighborhood':x["fields"]["objectid"]} for x in neighborhoods]

        new_neighborhoods.append([{'Geo Shape':x["fields"]["geo_shape"]} for x in neighborhoods])


        repo.dropCollection("meters-neighborhoods")
        repo.createCollection("meters-neighborhoods")
        repo['meters-neighborhoods'].insert_many(new_meters)
        repo['meters-neighborhoods'].insert_many(new_neighborhoods)


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
        repo.authenticate('medinad', 'medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('met', 'https://data.opendatasoft.com/explore/dataset/')
        
        this_script = doc.agent('alg:medinad#meters-neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('met:meters-neighborhoods', {'prov:label':'meters neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_metersneighbors = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_metersneighbors, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_meters, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #          'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        metersneighbors = doc.entity('dat:medinad#metersneighbors', {prov.model.PROV_LABEL:'METERS NEIGHBORS', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(metersneighbors, this_script)
        doc.wasGeneratedBy(metersneighbors, get_metersneighbors, endTime)
        doc.wasDerivedFrom(metersneighbors, resource, get_metersneighbors, get_metersneighbors, get_metersneighbors)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout() 
                  
        return doc

metersneighbors.execute()
doc = metersneighbors.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
