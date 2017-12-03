import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class aggpropValue(dml.Algorithm):
    contributor = 'carole07_echanglc_wongi'
    reads = ['carole07_echanglc_wongi.propValue']
    writes = ['carole07_echanglc_wongi.aggpropValue']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        repo.dropPermanent("aggpropValue")
        repo.createPermanent("aggpropValue")

        zipCount= []
        buildingCount= []
        
        for entry in repo.carole07_echanglc_wongi.propValue.find():
            if "zipcode" and "av_total" in entry:
                zipCount += [(entry["zipcode"], int(entry["av_total"]))]
                buildingCount += [(entry["zipcode"], 1)]
    
        #Aggregate transformation for zipCount
                
        keys = {r[0] for r in zipCount}
        aggregate_val= [(key, sum([v for (k,v) in zipCount if k == key])) for key in keys]

        #Aggregate transformation for buildingCount

        keys2 = {r[0] for r in buildingCount}
        aggregate_build= [(key, sum([v for (k,v) in buildingCount if k == key])) for key in keys2]

        #print("aggzip",aggregate_val)
        #print("aggbuild",aggregate_build)

        #Projection

        agg_build2= [(s,1/t) for (s,t) in aggregate_build]

        #Union transformation for agg_build2 and aggregate_val

        agg_both= aggregate_val + agg_build2

        keys3 = {r[0] for r in agg_both}
        aggregate_average= [(key, np.prod([v for (k,v) in agg_both if k == key])) for key in keys3]
        
        #print("aggregate_average", aggregate_average)
        
        final= []
        for entry in aggregate_average:
            final.append({'propZipcode:':entry[0], 'averagePropertyVal':entry[1]})

        #print("final", final)
        repo['carole07_echanglc_wongi.aggpropValue'].insert_many(final)

        #for entry in repo.carole07_echanglc_wongi.aggpropValue.find():
        #    print(entry)
             
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
        
        repo.dropPermanent("aggpropValue")
        repo.createPermanent("aggpropValue")
        
        zipCount= []
        buildingCount= []
        
        for entry in repo.carole07_echanglc_wongi.propValue.find():
            if "zipcode" and "av_total" in entry:
                zipCount += [(entry["zipcode"], int(entry["av_total"]))]
                buildingCount += [(entry["zipcode"], 1)]
        
        #Aggregate transformation for zipCount
        
        keys = {r[0] for r in zipCount}
        aggregate_val= [(key, sum([v for (k,v) in zipCount if k == key])) for key in keys]
        
        #Aggregate transformation for buildingCount
        
        keys2 = {r[0] for r in buildingCount}
        aggregate_build= [(key, sum([v for (k,v) in buildingCount if k == key])) for key in keys2]
        
        #print("aggzip",aggregate_val)
        #print("aggbuild",aggregate_build)
        
        #Projection
        
        agg_build2= [(s,1/t) for (s,t) in aggregate_build]
        
        #Union transformation for agg_build2 and aggregate_val
        
        agg_both= aggregate_val + agg_build2
        
        keys3 = {r[0] for r in agg_both}
        aggregate_average= [(key, np.prod([v for (k,v) in agg_both if k == key])) for key in keys3]
        
        #print("aggregate_average", aggregate_average)
        
        final= []
        for entry in aggregate_average:
            final.append({'propZipcode:':entry[0], 'averagePropertyVal':entry[1]})
        
        #print("final", final)
        repo['carole07_echanglc_wongi.aggpropValue'].insert_many(final)
        
        #for entry in repo.carole07_echanglc_wongi.aggpropValue.find():
        #    print(entry)
        
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
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdp1', 'https://data.nlc.org/resource/')
        doc.add_namespace('bdp2', 'https://data.boston.gov/export/622/208/')

        this_script = doc.agent('alg:carole07_echanglc_wongi#aggpropValue', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:carole07_echanglc_wongi#propValue', {'prov:label':'Property Value', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggProp = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggProp, this_script)
        doc.usage(get_aggProp, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        aggregateProperty = doc.entity('dat:carole07_echanglc_wongi#aggpropValue', {prov.model.PROV_LABEL:'Aggregate Properties', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggregateProperty, this_script)
        doc.wasGeneratedBy(aggregateProperty, get_aggProp, endTime)
        doc.wasDerivedFrom(aggregateProperty, resource_properties, get_aggProp, get_aggProp, get_aggProp)



        repo.logout()
                  
        return doc
