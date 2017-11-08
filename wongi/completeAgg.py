import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from collections import defaultdict

class completeAgg(dml.Algorithm):
    contributor = 'wongi'
    reads = ['wongi.schoolsAgg', 'wongi.camSchoolsAgg', 'wongi.aggpropValue', 'wongi.hospitalAgg', 'wongi.lightCoordinates']
    writes = ['wongi.completeAgg']
    
    def union(R, S):
        return R + S

    def difference(R, S):
        return [t for t in R if t not in S]

    def intersect(R, S):
        return [t for t in R if t in S]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]
     
    def product(R, S):
        return [(t,u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    def map(f, R):
        return [t for (k,v) in R for t in f(k,v)]
    
    def reduce(f, R):
        keys = {k for (k,v) in R}
        return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

    def combine(dictionaries):
        combined_dict = {}
        for dictionary in dictionaries:
            for key, value in dictionary.items():
                combined_dict.setdefault(key, []).append(value)
        return combined_dict
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wongi', 'wongi')

        repo.dropPermanent("completeAgg")
        repo.createPermanent("completeAgg")

        total = []
        for entry in repo.wongi.schoolsAgg.find():
            keys = list(entry)
            print(keys)
            total += [(entry[keys[1]], (keys[2],entry[keys[2]]))]
        for entry in repo.wongi.camSchoolsAgg.find():
            keys = list(entry)
            total += [(entry[keys[1]], (keys[2],entry[keys[2]]))]
        for entry in repo.wongi.aggpropValue.find():
            keys = list(entry)
            total += [(entry[keys[1]], (keys[2],entry[keys[2]]))]
        for entry in repo.wongi.hospitalAgg.find():
            keys = list(entry)
            total += [(entry[keys[1]], (keys[2],entry[keys[2]]))]
        for entry in repo.wongi.lightCoordinates.find():
            keys = list(entry)
            total += [(entry[keys[1]], (keys[2],entry[keys[2]]))]
        print(total)
        #Aggregate transformation for totalCount
                
        d = defaultdict(list)
        for key,value in total:
            d[key].append(value)
        final = dict(d)
        print(final)
        repo['wongi.completeAgg'].insert_one(final)
        
        for entry in repo.wongi.completeAgg.find():
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

        this_script = doc.agent('alg:wongi#completeAgg', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_camSchools = doc.entity('dat:wongi#camSchoolsAgg', {'prov:label':' Cam Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_schools = doc.entity('dat:wongi#schoolsAgg', {'prov:label':' Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_hospitals = doc.entity('dat:wongi#hospitalAgg', {'prov:label':' Hospitals Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_streetlights = doc.entity('dat:wongi#lightCoordinates', {'prov:label':' Streetlights Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_propValue = doc.entity('dat:wongi#aggpropValue', {'prov:label':' propValue Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_completeAgg = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_completeAgg, this_script)
        doc.usage(get_completeAgg, resource_camSchools, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(get_completeAgg, resource_schools, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_completeAgg, resource_hospitals, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_completeAgg, resource_streetlights, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_completeAgg, resource_propValue, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        completeAgg = doc.entity('dat:wongi#completeAgg', {prov.model.PROV_LABEL:' Complete Aggregate Zips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(completeAgg, this_script)
        doc.wasGeneratedBy(completeAgg, get_completeAgg, endTime)
        doc.wasDerivedFrom(completeAgg, resource_camSchools, get_completeAgg, get_completeAgg, get_completeAgg)
        doc.wasDerivedFrom(get_completeAgg, resource_schools, get_completeAgg, get_completeAgg, get_completeAgg)
        doc.wasDerivedFrom(get_completeAgg, resource_hospitals, get_completeAgg, get_completeAgg, get_completeAgg)
        doc.wasDerivedFrom(get_completeAgg, resource_streetlights, get_completeAgg, get_completeAgg, get_completeAgg)
        doc.wasDerivedFrom(get_completeAgg, resource_propValue, get_completeAgg, get_completeAgg, get_completeAgg)


        repo.logout()
                  
        return doc

