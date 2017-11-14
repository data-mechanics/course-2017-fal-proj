import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from operator import itemgetter
from collections import defaultdict

class developmentScore(dml.Algorithm):
    contributor = 'wongi'
    reads = ['wongi.schoolsAgg', 'wongi.camSchoolsAgg', 'wongi.aggpropValue', 'wongi.hospitalAgg', 'wongi.lightCoordinates', 'wongi.policeAgg']
    writes = ['wongi.developmentScore']
    
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

        repo.dropPermanent("developmentScore")
        repo.createPermanent("developmentScore")

        total = []
        propVal = []
        for entry in repo.wongi.schoolsAgg.find():
            keys = list(entry)
            total += [(entry[keys[1]], entry[keys[2]])]
        for entry in repo.wongi.camSchoolsAgg.find():
            keys = list(entry)
            total += [(entry[keys[1]], entry[keys[2]])]
        for entry in repo.wongi.aggpropValue.find():
            keys = list(entry)
            propVal += [(entry[keys[1]], entry[keys[2]])]
        for entry in repo.wongi.hospitalAgg.find():
            keys = list(entry)
            total += [(entry[keys[1]], entry[keys[2]])]
        for entry in repo.wongi.lightCoordinates.find():
            keys = list(entry)
            total += [(entry[keys[1]], entry[keys[2]])]
        for entry in repo.wongi.policeAgg.find():
            keys = list(entry)
            total += [(entry[keys[1]], entry[keys[2]])]
        #Aggregate transformation for totalCount

        keys = {r[0] for r in total}
        aggregate_val= [(key, sum([v for (k,v) in total if k == key])) for key in keys]
        
        divided_val = [(zipc, [val/ct for (z,val) in propVal if z == zipc]) for (zipc, ct) in aggregate_val]
        maxScore = max(divided_val,key=itemgetter(1))[1][0]
        removedEmpties = [v for v in divided_val if not v[1] in (None,0,[],'')]
        minScore = min(removedEmpties,key=itemgetter(1))[1][0]

        a = (100 - 0) / (maxScore - minScore)
        b = 100 - a * maxScore
        final= []
        for entry in divided_val:
            score = [a*i+b for i in entry[1]]
            final.append({'zipcode:':entry[0], 'devScore':score})

        repo['wongi.developmentScore'].insert_many(final)
        
        for entry in repo.wongi.developmentScore.find():
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

        this_script = doc.agent('alg:wongi#developmentScore', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_camSchools = doc.entity('dat:wongi#camSchoolsAgg', {'prov:label':' Cam Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_schools = doc.entity('dat:wongi#schoolsAgg', {'prov:label':' Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_hospitals = doc.entity('dat:wongi#hospitalAgg', {'prov:label':' Hospitals Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_streetlights = doc.entity('dat:wongi#lightCoordinates', {'prov:label':' Streetlights Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_propValue = doc.entity('dat:wongi#aggpropValue', {'prov:label':' propValue Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_polices = doc.entity('dat:wongi#policeAgg', {'prov:label':' Police Stations Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_developmentScore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_developmentScore, this_script)
        doc.usage(get_developmentScore, resource_camSchools, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(get_developmentScore, resource_schools, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_developmentScore, resource_hospitals, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_developmentScore, resource_streetlights, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_developmentScore, resource_propValue, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_developmentScore, resource_polices, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        developmentScore = doc.entity('dat:wongi#developmentScore', {prov.model.PROV_LABEL:' Complete Dev Scores', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(developmentScore, this_script)
        doc.wasGeneratedBy(developmentScore, get_developmentScore, endTime)
        doc.wasDerivedFrom(developmentScore, resource_camSchools, get_developmentScore, get_developmentScore, get_developmentScore)
        doc.wasDerivedFrom(get_developmentScore, resource_schools, get_developmentScore, get_developmentScore, get_developmentScore)
        doc.wasDerivedFrom(get_developmentScore, resource_hospitals, get_developmentScore, get_developmentScore, get_developmentScore)
        doc.wasDerivedFrom(get_developmentScore, resource_streetlights, get_developmentScore, get_developmentScore, get_developmentScore)
        doc.wasDerivedFrom(get_developmentScore, resource_propValue, get_developmentScore, get_developmentScore, get_developmentScore)
        doc.wasDerivedFrom(get_developmentScore, resource_polices, get_developmentScore, get_developmentScore, get_developmentScore)


        repo.logout()
                  
        return doc

