from geopy.geocoders import Nominatim
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class lightCoordinates(dml.Algorithm):
    contributor = 'carole07_echanglc_wongi'
    reads = ['carole07_echanglc_wongi.lightTransform']
    writes = ['carole07_echanglc_wongi.lightCoordinates']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')

        repo.dropPermanent("lightCoordinates")
        repo.createPermanent("lightCoordinates")

        zipCount= []
        geolocator = Nominatim()
        for entry in repo.carole07_echanglc_wongi.lightTransform.find():
            #print(entry)
            if "Long:" and "Lat:" in entry:
                #print(entry)
                long = entry["Long:"]
                lat = entry["Lat:"]
                location = geolocator.reverse((lat, long)).raw
                zipcode = location["address"]["postcode"]
                zipCount += [(zipcode, 1)]
    
        #Aggregate transformation for zipCount
                
        keys = {r[0] for r in zipCount}
        aggregate_val= [(key, sum([v for (k,v) in zipCount if k == key])) for key in keys]

        final= []
        for entry in aggregate_val:
            final.append({'lightZipcode:':entry[0], 'lightCount':entry[1]})
        
        repo['carole07_echanglc_wongi.lightCoordinates'].insert_many(final)
        
        #for entry in repo.carole07_echanglc_wongi.lightCoordinates.find():
        #     print(entry)
             
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
        
        repo.dropPermanent("lightCoordinates")
        repo.createPermanent("lightCoordinates")
        
        zipCount= []
        geolocator = Nominatim()
        for entry in repo.carole07_echanglc_wongi.lightTransform.find():
            #print(entry)
            if "Long:" and "Lat:" in entry:
                #print(entry)
                long = entry["Long:"]
                lat = entry["Lat:"]
                location = geolocator.reverse((lat, long)).raw
                zipcode = location["address"]["postcode"]
                zipCount += [(zipcode, 1)]
        
        #Aggregate transformation for zipCount
        
        keys = {r[0] for r in zipCount}
        aggregate_val= [(key, sum([v for (k,v) in zipCount if k == key])) for key in keys]
        
        final= []
        for entry in aggregate_val:
            final.append({'lightZipcode:':entry[0], 'lightCount':entry[1]})
                
        repo['carole07_echanglc_wongi.lightCoordinates'].insert_many(final)
        
        #for entry in repo.carole07_echanglc_wongi.lightCoordinates.find():
        #     print(entry)
                
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

        this_script = doc.agent('alg:carole07_echanglc_wongi#lightCoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:carole07_echanglc_wongi#lightTransform', {'prov:label':'Light Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggLights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggLights, this_script)
        doc.usage(get_aggLights, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        aggregateLights = doc.entity('dat:carole07_echanglc_wongi#lightCoordinates', {prov.model.PROV_LABEL:'Light Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggregateLights, this_script)
        doc.wasGeneratedBy(aggregateLights, get_aggLights, endTime)
        doc.wasDerivedFrom(aggregateLights, resource_properties, get_aggLights, get_aggLights, get_aggLights)



        repo.logout()
                  
        return doc

