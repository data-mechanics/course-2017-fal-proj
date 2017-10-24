import urllib.request

import json

import dml

import prov.model

import datetime

import uuid




class crimeTransformation(dml.Algorithm):

     contributor = 'francisz_jrashaan'

     reads = ['francisz_jrashaan.crime']

     writes = ['francisz_jrashaan.crimeData']


     @staticmethod

     def execute(trial = False):

         '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

         startTime = datetime.datetime.now()



         # Set up the database connection.

         client = dml.pymongo.MongoClient()

         repo = client.repo

         repo.authenticate('francisz_jrashaan','francisz_jrashaan')



         repo.dropPermanent("crimeData")

         repo.createPermanent("crimeData")



         homicides = []

         homicideCount = 0




         #selection to get homicides
         for entry in repo.francisz_jrashaan.crime.find():

            if entry['OFFENSE_CODE_GROUP'] == 'Homicide':
                 homicides.append(entry)
                 homicideCount+= 1
                     

         #projection to simplify data to obtain street, lat, long, and date
         homicideCrimes = []
         for entry in homicides:
             x = lambda t: ({'STREET:':t['STREET'],'OFFENSE_DESCRIPTION:':t['OFFENSE_DESCRIPTION'],'OCCURED_ON_DATE:':t['OCCURRED_ON_DATE'],'Long:':t['Long'],'Lat:':t['Lat']})
             y = x(entry)
             homicideCrimes.append(y)
         
         for entry in homicideCrimes:
             print(entry)
             print("right form")
         
        
        # print("homicide crimes with street, offense, date, lat, and long",homicideCrimes)





         print("homicides in Boston", homicideCrimes)
         repo['francisz_jrashaan.crimeData'].insert_many(homicideCrimes)
         

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



         this_script = doc.agent('alg:francisz_jrashaan#crimeTransformation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


         #add url on doc.entity
         resource_selectProject = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Data Set', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

         do_selectProject = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

         doc.wasAssociatedWith(do_selectProject, this_script)

         doc.usage(do_selectProject, resource_selectProject, startTime, None,

                   {prov.model.PROV_TYPE:'ont:Retrieval'})







         selectProject = doc.entity('dat:francisz_jrashaan#crimeData', {prov.model.PROV_LABEL:'Data Set which is  Selected and Projected', prov.model.PROV_TYPE:'ont:DataSet'})

         doc.wasAttributedTo(selectProject, this_script)

         doc.wasGeneratedBy(selectProject, do_selectProject, endTime)

         doc.wasDerivedFrom(selectProject, resource_selectProject, do_selectProject, do_selectProject, do_selectProject)







         repo.logout()



         return doc


crimeTransformation.execute()

doc = crimeTransformation.provenance()

print(doc.get_provn())

print(json.dumps(json.loads(doc.serialize()), indent=4))
