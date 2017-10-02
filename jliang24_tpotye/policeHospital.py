import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

class policeHospital(dml.Algorithm):
    contributor = 'jliang24_tpotye'
    reads = ['jliang24_tpotye.police', 'jliang24_tpotye.hospital']
    writes = ['jliang24_tpotye.policeHospital']
    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jliang24_tpotye', 'jliang24_tpotye')

        repo.dropPermanent("policeHospital")
        repo.createPermanent("policeHospital")

        numPolice= []
        numHospital= []
        
        for entry in repo.jliang24_tpotye.police.find():
                if "location_zip" in entry:
                    numPolice += [(entry["location_zip"], 1)]

        for entry in repo.jliang24_tpotye.hospital.find():
                if "ZIPCODE" in entry:
                    numHospital += [("0"+ entry["ZIPCODE"], 1)]
        
    
        union_both= numPolice + numHospital
        
        #Aggregate transformation for valueZip
                
        keys = {r[0] for r in union_both}
        aggregate_both= [(key, sum([v for (k,v) in union_both if k == key])) for key in keys]

        print(aggregate_both)
        
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

        this_script = doc.agent('alg:jliang24_tpotye#getdata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_aggpolice = doc.entity('bdp:wc8w-nujj', {'prov:label':'Aggregate Properties', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_aggpolice = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_aggpolice, this_script)
        doc.usage(get_aggpolice, resource_aggpolice, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})



        aggregatePolice = doc.entity('dat:jliang24_tpotye#police', {prov.model.PROV_LABEL:'Police', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggregatePolice, this_script)
        doc.wasGeneratedBy(aggregatePolice, get_aggpolice, endTime)
        doc.wasDerivedFrom(aggregatePolice, resource_aggpolice, get_aggpolice, get_aggpolice, get_aggpolice)
        

        resource_agghospital = doc.entity('bdp:wc8w-nujj', {'prov:label':'hospital', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_agghospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_agghospital, this_script)
        doc.usage(get_agghospital, resource_agghospital, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})



        aggregateHospital = doc.entity('dat:jliang24_tpotye#hospital', {prov.model.PROV_LABEL:'Hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggregateHospital, this_script)
        doc.wasGeneratedBy(aggregateHospital, get_agghospital, endTime)
        doc.wasDerivedFrom(aggregateHospital, resource_agghospital, get_agghospital, get_agghospital, get_agghospital)



        repo.logout()
                  
        return doc

policeHospital.execute()
doc = policeHospital.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
