import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict

from collections import Counter

class hospital_insufficient_transformation(dml.Algorithm):
    contributor = 'lmy1031_zhuoshu'
    reads = ['lmy1031_zhuoshu.emergency', 'lmy1031_zhuoshu.neighbor']
    writes = ['lmy1031_zhuoshu.insufficient']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Start connecting database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')  
        emergency1 = repo.lmy1031_zhuoshu.emergency.find()
        neighbor = repo.lmy1031_zhuoshu.neighbor.find()
        #make the cursor to dict
        y   = []
        
        for i in neighbor:
            y.append(i)
        neighbor=y[0]
        #delete useless key
        neighbor.pop('_id', None)

        
       
        
        x   = []
        
        for i in emergency1:
            x.append(i)
        x=x[0]
        #delete the useless key
        x.pop('_id', None)
        x.pop('0Zip', None)
        print(x)
        
        
        #iterate neighbor and emergency several times to find the ratio of hospital/garden in specific zone
        ratio={}
        
        for key in (x):
            numberOfhospital=0
            if (x[key][0]!=0):
                for key2 in (neighbor):
                    if key==key2:
                        for i in neighbor[key]:
                            if i in x:
                                numberOfhospital+=x[i][1]
                ratio.setdefault(key,numberOfhospital/x[key][0])
       
        insufficientHospital=[]
        deleteList=[]
        for i in ratio:
            if (ratio[i]>0.5):
                deleteList+=[i]
 
        for i in deleteList:
            ratio.pop(i,None)
        
        
        print("the following zipcode should build more hospitals: ",ratio)
        repo.dropCollection("insufficient")
        repo.createCollection("garden")
        repo['lmy1031_zhuoshu.insufficient'].insert([ratio])
                    
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

        this_script = doc.agent('alg:#insufficientzipcode', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        neighbor = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.neighbor', {prov.model.PROV_LABEL:'neighbor', prov.model.PROV_TYPE:'ont:DataSet'})
        emergency1= doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.emergency', {prov.model.PROV_LABEL:'emergnecy', prov.model.PROV_TYPE:'ont:DataSet'})

        hospital_insufficient_transformation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(hospital_insufficient_transformation, this_script)
        
        doc.usage(hospital_insufficient_transformation, neighbor, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(hospital_insufficient_transformation, emergency1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        
        insufficient= doc.entity('dat:#insufficient',
                                    {prov.model.PROV_LABEL:'insufficient',
                                     prov.model.PROV_TYPE:'ont:DataSet'})

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(insufficient, this_script)
        doc.wasGeneratedBy(insufficient, hospital_insufficient_transformation, endTime)
        doc.wasDerivedFrom(insufficient, neighbor, hospital_insufficient_transformation, hospital_insufficient_transformation, hospital_insufficient_transformation)
        doc.wasDerivedFrom(insufficient, emergency1, hospital_insufficient_transformation, hospital_insufficient_transformation, hospital_insufficient_transformation)
        


        repo.logout()
                  
        return doc

hospital_insufficient_transformation.execute()
doc = hospital_insufficient_transformation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
