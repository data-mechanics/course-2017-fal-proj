import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class policeAgg(dml.Algorithm):
    contributor = 'wongi'
    reads = ['wongi.polices']
    writes = ['wongi.policeAgg']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wongi', 'wongi')

        repo.dropPermanent("policeAgg")
        repo.createPermanent("policeAgg")

        zipCount= []
        for entry in repo.wongi.polices.find():
            for entry2 in entry["data"]["fields"]:
                if entry2["name"] == "ZIP":
                    for entry3 in entry2["statistics"]["values"]:
                        zipc = entry3["value"]
                        count = entry3["count"]
                        zipCount += [(zipc, count)]

         #Aggregate transformation for zipCount
                
        keys = {r[0] for r in zipCount}
        aggregate_val= [(key, sum([v for (k,v) in zipCount if k == key])) for key in keys]

        final= []
        for entry in aggregate_val:
            final.append({'policesZipcode:':entry[0], 'policesCount':entry[1]})

        repo['wongi.policeAgg'].insert_many(final)
        
        for entry in repo.wongi.policeAgg.find():
             print(entry)
             
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
        doc.add_namespace('bdp1', 'https://data.nlc.org/resource/')
        doc.add_namespace('bdp2', 'https://data.boston.gov/export/622/208/')

        this_script = doc.agent('alg:wongi#policeAgg', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:wongi#polices', {'prov:label':'polices Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggpolices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggpolices, this_script)
        doc.usage(get_aggpolices, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        aggregatepolices = doc.entity('dat:wongi#policeAgg', {prov.model.PROV_LABEL:'polices Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggregatepolices, this_script)
        doc.wasGeneratedBy(aggregatepolices, get_aggpolices, endTime)
        doc.wasDerivedFrom(aggregatepolices, resource_properties, get_aggpolices, get_aggpolices, get_aggpolices)



        repo.logout()
                  
        return doc

