import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict

from collections import Counter

class hospital_garden_transformation(dml.Algorithm):
    contributor = 'lmy1031_zhuoshu'
    reads = ['lmy1031_zhuoshu.hospital', 'lmy1031_zhuoshu.garden']
    writes = ['lmy1031_zhuoshu.emergency']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Start connecting database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')
        bostonHospital = repo.lmy1031_zhuoshu.hospital.find()
        bostonGarden = repo.lmy1031_zhuoshu.garden.find()
        
        
        print("start hospital_garden transformation!")
        
        #result = []
        zipcode_garden = []
        for i in bostonGarden[1:]:
            if "zip_code" in i:
                #print(i['zip_code'])
                zipcode_garden.append("0" + i["zip_code"])
        #print(result1)
        
        zipcode_hospital = []
        for i in bostonHospital:
            if "ZIPCODE" in i:
                zipcode_hospital.append("0" + i["ZIPCODE"])


        result_garden = dict(Counter(zipcode_garden))
        result_hospital = dict(Counter(zipcode_hospital))
        result={}
        print(result_hospital)
        #find the number of hospitals and gardens
        for key in (result_garden.keys()):
            if key in result_hospital:
                result.setdefault(key, [result_garden[key]]).append(result_hospital[key])
            else:
                result.setdefault(key, [result_garden[key]]).append(0)

        for key in (result_hospital.keys()):
            if key not in result_garden:
                result.setdefault(key,[0]).append(result_hospital[key])
        
        repo.dropCollection("emergency")
        repo.createCollection("emergency")
        repo['lmy1031_zhuoshu.emergency'].insert([result])

        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:#hospital_garden_transformation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        boston_hospital = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu_hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        boston_garden = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu_garden', {prov.model.PROV_LABEL:'garden', prov.model.PROV_TYPE:'ont:DataSet'})
        
        boston_garden_hospital_emergency = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(boston_garden_hospital_emergency, this_script)
        
        doc.usage(boston_garden_hospital_emergency, boston_hospital, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(boston_garden_hospital_emergency, boston_garden, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
                  
        boston_emergency = doc.entity('dat:#emergency',
                                        {prov.model.PROV_LABEL:'emergency',
                                        prov.model.PROV_TYPE:'ont:DataSet'})
                 
        doc.wasAttributedTo(boston_emergency, this_script)
        doc.wasGeneratedBy(boston_emergency, boston_garden_hospital_emergency, endTime)
        doc.wasDerivedFrom(boston_emergency, boston_hospital, boston_garden_hospital_emergency, boston_garden_hospital_emergency, boston_garden_hospital_emergency)
        doc.wasDerivedFrom(boston_emergency, boston_garden, boston_garden_hospital_emergency, boston_garden_hospital_emergency, boston_garden_hospital_emergency)
                  
                  
        repo.logout()
                  
        return doc

hospital_garden_transformation.execute()
doc = hospital_garden_transformation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
