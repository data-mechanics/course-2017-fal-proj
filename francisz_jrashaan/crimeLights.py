import urllib.request

import json

import dml

import prov.model

import datetime

import uuid




class crimeLights(dml.Algorithm):

     contributor = 'francisz_jrashaan'

     reads = ['francisz_jrashaan.crimeData','francisz_jrashaan.newLights']

     writes = ['francisz_jrashaan.crimeLight']





     @staticmethod

     def execute(trial = False):

         '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

         startTime = datetime.datetime.now()



         # Set up the database connection.

         client = dml.pymongo.MongoClient()

         repo = client.repo

         repo.authenticate('francisz_jrashaan','francisz_jrashaan')



         repo.dropPermanent("crimeLights")

         repo.createPermanent("crimeLights")



         crimeLatLong = []
         lightLatLong = []
         lightMurders = []
         
         x =  repo.francisz_jrashaan.crimeData.find()
                       
       
         
         
         for entry in repo.francisz_jrashaan.crimeData.find():

# print(entry)
                if entry['Long:'] == None or entry['Lat:'] == None:
                   continue
                
                a = entry['Long:']
                b = a[:6]
                c = (entry['Lat:'])
                d = c[:5]
                crimeLatLong.append(tuple((b,d)))
         
         
         
         
         for entry in repo.francisz_jrashaan.newLights.find():
                if entry['Long:'] == None or entry['Lat:'] == None:
                   continue
                a = entry['Long:']
                b = a[:6]
                c = (entry['Lat:'])
                d = c[:5]

                lightLatLong.append(tuple((b,d)))



         for x in crimeLatLong:
             for y in lightLatLong:
                
                 if x == y:
                  
                  lightMurders.append([tuple(x),1])
                  break
        #aggregate
         keys = {x[0] for x in lightMurders}

         aggregate = [(key,sum([v for (k,v) in lightMurders if k == key])) for key in keys]
         agg = []
         for x in aggregate:
             y = lambda t: ({'Long':t[0][0],'Lat':t[0][1],'Intersections':str(t[1])})
             z = y(x)
             agg.append(z)
             

         
         repo['francisz_jrashaan.crimeLight'].insert_many(agg)



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

         repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')

         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.

         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.

         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')






         resource_agg2 = doc.entity('bdp:wc8w-nujj', {'prov:label':'Longitude and Latitude of Lights in City', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    
    









         this_script = doc.agent('dat:francisz_jrashaan#crimeLights', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
         
         compute_aggLights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

         aggLights = doc.entity('dat:francisz_jrashaan#newLights', {prov.model.PROV_LABEL:'Aggregation second half ', prov.model.PROV_TYPE:'ont:DataSet'})
         
         lightsCrime = doc.entity('dat:francisz_jrashaan#crimeLight', {prov.model.PROV_LABEL:'Aggregation second half ', prov.model.PROV_TYPE:'ont:DataSet'})
         
         newCrime = doc.entity('dat:francisz_jrashaan#crimeData', {prov.model.PROV_LABEL:'Aggregation second half ', prov.model.PROV_TYPE:'ont:DataSet'})
         
         doc.wasAssociatedWith(compute_aggLights, this_script)
         doc.wasAttributedTo(newCrime, this_script)
         doc.wasGeneratedBy(newCrime, compute_aggLights, endTime)
         doc.wasDerivedFrom(newCrime, lightsCrime, compute_aggLights, compute_aggLights, compute_aggLights)
         doc.wasDerivedFrom(newCrime, aggLights, compute_aggLights, compute_aggLights, compute_aggLights)
         
         
         
         doc.usage(compute_aggLights, aggLights, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
         doc.usage(compute_aggLights, lightsCrime, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})







         
         
         
         
         
         
         
         
         
         
 























         repo.logout()



         return doc



crimeLights.execute()

doc = crimeLights.provenance()

print(doc.get_provn())

print(json.dumps(json.loads(doc.serialize()), indent=4))
