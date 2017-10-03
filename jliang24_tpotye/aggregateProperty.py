import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class aggregateProperty(dml.Algorithm):
    contributor = 'jliang24_tpotye'
    reads = ['jliang24_tpotye.properties']
    writes = ['jliang24_tpotye.aggregatePropertyData']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jliang24_tpotye', 'jliang24_tpotye')

        repo.dropPermanent("aggregatePropertyData")
        repo.createPermanent("aggregatePropertyData")

        valueZip= []
        numBuild= []
        
        for entry in repo.jliang24_tpotye.properties.find():
            if "zipcode" and "av_total" in entry:
                valueZip += [(entry["zipcode"], int(entry["av_total"]))]
                numBuild += [(entry["zipcode"], 1)]
    
        #Aggregate transformation for valueZip
                
        keys = {r[0] for r in valueZip}
        aggregate_val= [(key, sum([v for (k,v) in valueZip if k == key])) for key in keys]

        #Aggregate transformation for numBuild

        keys2 = {r[0] for r in numBuild}
        aggregate_build= [(key, sum([v for (k,v) in numBuild if k == key])) for key in keys2]

        print("aggzip",aggregate_val)
        print("aggbuild",aggregate_build)

        #Projection

        agg_build2= [(s,1/t) for (s,t) in aggregate_build]

        #Union transformation for agg_build2 and aggregate_val

        agg_both= aggregate_val + agg_build2

        keys3 = {r[0] for r in agg_both}
        aggregate_average= [(key, np.prod([v for (k,v) in agg_both if k == key])) for key in keys3]
        
        print("aggregate_average", aggregate_average)
        
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
        repo.authenticate('jliang24_tpotye', 'jliang24_tpotye')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jliang24_tpotye#aggregateProperty', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Assessments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggProp = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggProp, this_script)
        doc.usage(get_aggProp, resource_properties, startTime)



        aggregateProperty = doc.entity('dat:jliang24_tpotye#aggregateProperty', {prov.model.PROV_LABEL:'Aggregate Properties', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggregateProperty, this_script)
        doc.wasGeneratedBy(aggregateProperty, get_aggProp, endTime)
        doc.wasDerivedFrom(aggregateProperty, resource_properties, get_aggProp, get_aggProp, get_aggProp)



        repo.logout()
                  
        return doc

aggregateProperty.execute()
doc = aggregateProperty.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
