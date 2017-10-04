import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class potholeAnalysis(dml.Algorithm):
    contributor = 'jliang24_tpotye'
    reads = ['jliang24_tpotye.doc_311', 'jliang24_tpotye.potholes']
    writes = ['jliang24_tpotye.potholeAnalysisData']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jliang24_tpotye', 'jliang24_tpotye')

        repo.dropPermanent("potholeAnalysisData")
        repo.createPermanent("potholeAnalysisData")

        potholes311= []
        potholeRepairs= []

        for entry in repo.jliang24_tpotye.doc_311.find():
            if "location_zipcode" in entry:
               potholes311 += [(entry["case_title"], entry["location_zipcode"])]

        for entry in repo.jliang24_tpotye.potholes.find():
            if "location_zipcode" in entry:
                potholeRepairs += [("0"+ entry["location_zipcode"], 1)]
    
        # Selection transformation for 311
        
        filter_potholes311= [(t,1) for (k,t) in potholes311 if lambda k: k== "Request for Pothole Repair"]

        # Aggregate transformation for 311
        keys = {r[0] for r in filter_potholes311}
        aggregate_potholes311= [(key, sum([v for (k,v) in filter_potholes311 if k == key])) for key in keys]

        #Aggregate transformation for pothole repairs
        keys2 = {r[0] for r in potholeRepairs}
        aggregate_potholes= [(key, sum([v for (k,v) in potholeRepairs if k == key])) for key in keys2]
        
        
        print("aggregate_potholes311",aggregate_potholes311)
        print("aggregate_potholes",aggregate_potholes)

        #Projection

        pro_311= [(s,1/t) for (s,t) in aggregate_potholes311]

        #Union transformation to find ratio of repairs to requests

        both= aggregate_potholes + pro_311

        keys3 = {r[0] for r in both}
        ratio= [(key, np.prod([v for (k,v) in both if k == key])) for key in keys3]

        print("ratio", ratio)


        final2= []
        for entry in ratio:
            final2.append({'zipcode:':entry[0], 'ratio':entry[1]})

        print("final", final2)

        repo['jliang24_tpotye.potholeAnalysisData'].insert_many(final2)
                            
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
        doc.add_namespace('bdp1', 'https://data.nlc.org/resource/')

        this_script = doc.agent('alg:jliang24_tpotye#potholeAnalysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_311 = doc.entity('bdp:wc8w-nujj', {'prov:label':'311 Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        resource_potholes = doc.entity('bdp1:5udy-aqqy', {'prov:label':'Potholes Repaired', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_potholes_analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_potholes_analysis, this_script)
        
        doc.usage(get_potholes_analysis, resource_311, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(get_potholes_analysis, resource_potholes, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})

        potholes_analysis = doc.entity('dat:jliang24_tpotye#potholeAnalysisData', {prov.model.PROV_LABEL:'Pothole Analysis', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(potholes_analysis, this_script)
        doc.wasGeneratedBy(potholes_analysis, get_potholes_analysis, endTime)
        doc.wasDerivedFrom(potholes_analysis, resource_311, get_potholes_analysis, get_potholes_analysis, get_potholes_analysis)
        doc.wasDerivedFrom(potholes_analysis, resource_potholes, get_potholes_analysis, get_potholes_analysis, get_potholes_analysis)


        repo.logout()
                  
        return doc

potholeAnalysis.execute()
doc = potholeAnalysis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
