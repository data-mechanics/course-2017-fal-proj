import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class lightTransform(dml.Algorithm):
     contributor = 'wongi'
     reads = ['wongi.streetlights']
     writes = ['wongi.lightTransform']





     @staticmethod

     def execute(trial = False):

         '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

         startTime = datetime.datetime.now()

         # Set up the database connection.

         client = dml.pymongo.MongoClient()

         repo = client.repo

         repo.authenticate('wongi','wongi')



         repo.dropPermanent("lightTransform")

         repo.createPermanent("lightTransform")








         lights = []
         #projection to get coordinates
         
         for entry in repo.wongi.streetlights.find():
             x = lambda t: ({'Long:':t['Long'],'Lat:':t['Lat']})
             y = x(entry)
             lights.append(y)




         repo['wongi.lightTransform'].insert_many(lights)

         #print("streetlight coordinates", lights)
         


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

         repo.authenticate('wongi', 'wongi')

         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.

         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.

         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')



         this_script = doc.agent('alg:wongi#lightTransform', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})



         resource_project = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5', {'prov:label':'Dataset which is projected', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

         get_project = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

         doc.wasAssociatedWith(get_project, this_script)

         doc.usage(get_project, resource_project, startTime, None,

                   {prov.model.PROV_TYPE:'ont:Retrieval'})







         project = doc.entity('dat:wongi#lightTransform', {prov.model.PROV_LABEL:'New Dataset after Projection', prov.model.PROV_TYPE:'ont:DataSet'})

         doc.wasAttributedTo(project, this_script)

         doc.wasGeneratedBy(project, get_project, endTime)

         doc.wasDerivedFrom(project, resource_project, get_project, get_project, get_project)







         repo.logout()



         return doc
