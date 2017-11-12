import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict 

class policeneighbors(dml.Algorithm):
    contributor = 'medinad'
    reads = ['medinad.police','medinad.neighbor']
    writes = ['medinad.police-neighborhoods']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')


        police = repo.medinad.police
        neighborhoods = repo.medinad.neighbor
        

        def product(R, S):
            return [(t,u) for t in R for u in S]

        def project(R, p):
            return [p(t) for t in R]

        police_list = list(police.find())
        #print(meters1_list)
        neighborhoods_list = list(neighborhoods.find())

        new_police = []

        new_police = [{'Station Name':x['name'],"Address":x["location_location"],"Point":x["location"]["coordinates"]} for x in police_list]

        #print(new_meters)        

        #new_meters.extend()
        nid_list = [{'Neighborhood':x["fields"]["objectid"], 'Geo Shape':x["fields"]["geo_shape"]} for x in neighborhoods_list]#, 'Geo Shape':x["fields"]["geo_shape"]
        #print(nid_list)

        productmn = product(new_police,nid_list)
        
        #print(productmn[0])

        
        print(type(productmn))
        #ngeo_list = [{} for x in neighborhoods_list]

        final_dict = [{'Police':n, 'Neighborhood':d} for (n,d) in productmn]
       
        for i in range(2):
            print(final_dict[i])


        


        #final_dict['Meters'] = [{'Meter':productmn['Meters']} for x in productmn['Meters']] 

        

        #for x in productmn:
            #print(x[0])
         
        #print(type(new_meters))



        repo.dropCollection("medinad.police-neighborhoods")
        repo.createCollection("medinad.police-neighborhoods")
#       repo['medinad.meters-neighborhoods'].insert_many(new_meters)
        repo['medinad.police-neighborhoods'].insert_many(final_dict)



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
        pass 
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mtn', 'https://data.opendatasoft.com/explore/dataset/')
        
        this_script = doc.agent('mtn:medinad#meters-neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mtn:meters-neighborhoods', {'prov:label':'meters neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_metersneighbors = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_metersneighbors, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_metersneighbors, resource, startTime, None,
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

policeneighbors.execute()
doc = policeneighbors.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
