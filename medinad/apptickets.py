import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict 

class apptickets(dml.Algorithm):
    contributor = 'medinad'
    reads = ['medinad.app','medinad.tickets']
    writes = ['medinad.app-tickets']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')


        app1 = repo.medinad.app
        tickets = repo.medinad.tickets
        

        def product(R, S):
            return [(t,u) for t in R for u in S]

        def project(R, p):
            return [p(t) for t in R]

        app1_list = list(app1.find())
        #print(meters1_list)
        tickets_list = list(tickets.find())

        new_ticket = [{'Day':x["Day Index"], 'Page views': x["Pageviews"]} for x in tickets_list]

        new_app = []

        new_app = [{ 'Zone':x['Zone Name'] ,'January':x["January"],'February':x["February"],'March':x["March"],'April':x["April"],'May':x["May"],'June':x["June"],'July':x["July"]} for x in app1_list]

        #print(new_meters)        

        #new_meters.extend()
        #nid_list = [{'Neighborhood':x["fields"]["objectid"], 'Geo Shape':x["fields"]["geo_shape"]} for x in tickets_list]
        #print(nid_list)

        productmn = product(new_app,new_ticket)  
        
        #print(productmn[0])

        
        print(type(productmn))
        #ngeo_list = [{} for x in neighborhoods_list]

        final_dict = [{'App':n, 'Tickets':d} for (n,d) in productmn]
       
        for i in range(2):
            print(final_dict[i])


        


        #final_dict['Meters'] = [{'Meter':productmn['Meters']} for x in productmn['Meters']] 

        

        #for x in productmn:
            #print(x[0])
         
        



        repo.dropCollection("medinad.app-tickets")
        repo.createCollection("medinad.app-tickets")
#       repo['medinad.meters-neighborhoods'].insert_many(new_meters)
        repo['medinad.app-tickets'].insert_many(final_dict)



        #repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        ''' 
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        pass
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('medinad', 'medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mtn', 'https://data.opendatasoft.com/explore/dataset/')
        
        this_script = doc.agent('alg:medinad#app-tickets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mtn:app-tickets', {'prov:label':'app tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_apptickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_apptickets, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_apptickets, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #          'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        apptickets = doc.entity('dat:medinad#apptickets', {prov.model.PROV_LABEL:'APP TICKETS', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(apptickets, this_script)
        doc.wasGeneratedBy(apptickets, get_apptickets, endTime)
        doc.wasDerivedFrom(apptickets, resource, get_apptickets, get_apptickets, get_apptickets)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout() 
                  
        return doc

apptickets.execute()
doc = apptickets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
