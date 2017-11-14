import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict

from collections import Counter
#helper function to return updated collection of ratio
def helperfunction(x,y):
    ratio={}
        
    for key in (x):
        numberOfhospital=0
        if (x[key][0]!=0):
            for key2 in (y):
                if key==key2:
                    for i in y[key]:
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
    return (ratio)


class addHospital(dml.Algorithm):
    contributor = 'lmy1031_zhuoshu'
    reads = ['lmy1031_zhuoshu.insuciffient', 'lmy1031_zhuoshu.neighbor','lmy1031_zhuoshu.emergency']
    writes = ['lmy1031_zhuoshu.addhospital']
    
    
    
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Start connecting database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')  
        insufficient = repo.lmy1031_zhuoshu.insufficient.find()
        neighbor = repo.lmy1031_zhuoshu.neighbor.find()
        emergency=repo.lmy1031_zhuoshu.emergency.find()
        #transfer coursor to dict
        y   = []
        for i in neighbor:
            y.append(i)
        neighbor=y[0]
        neighbor.pop('_id', None)
        
        
        x   = []
        for i in emergency:
            x.append(i)
        emergency=x[0]
        emergency.pop('_id', None)
        emergency.pop('0Zip', None)
        
        
        z   = []
        for i in insufficient:
            z.append(i)
    
        insufficient=z[0]
        insufficient.pop('_id', None)
        
        
        totalScore=0
        finaldict={}
        placeToadd="";
        for i in insufficient:
            emergency[i][1]=emergency[i][1]+1
            dict=helperfunction(emergency,neighbor)
            
            score=0
            for j in dict:
                score+=dict[j]
            if score>totalScore:
                totalScore=score
                placeToadd=i
                finaldict=dict
            emergency[i][1]=emergency[i][1]-1
        print (placeToadd)
        addhospital=emergency
        addhospital[placeToadd][1]+=1
        print(addhospital)
    
        
        
        repo.dropCollection("addHospital")
        repo.createCollection("addHospital")
        repo['lmy1031_zhuoshu.addHospital'].insert([addhospital])
                    
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

        this_script = doc.agent('alg:#addHospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        insufficient = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.insufficient', {prov.model.PROV_LABEL:'insufficient', prov.model.PROV_TYPE:'ont:DataSet'})
        neighbor = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.neighbor', {prov.model.PROV_LABEL:'neighbor', prov.model.PROV_TYPE:'ont:DataSet'})
        
        emergency = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.emergency', {prov.model.PROV_LABEL:'emergency', prov.model.PROV_TYPE:'ont:DataSet'})

        addHospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(addHospital, this_script)
        
        doc.usage(addHospital, neighbor, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(addHospital, emergency, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(addHospital, emergency, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        
        morehospital = doc.entity('dat:#morehospital',
                                    {prov.model.PROV_LABEL:'morehospital',
                                     prov.model.PROV_TYPE:'ont:DataSet'})

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(morehospital, this_script)
        doc.wasGeneratedBy(morehospital, addHospital, endTime)
        doc.wasDerivedFrom(morehospital, emergency, addHospital, addHospital, addHospital)
        doc.wasDerivedFrom(morehospital, neighbor, addHospital, addHospital, addHospital)
        doc.wasDerivedFrom(morehospital, insufficient, addHospital, addHospital, addHospital)


        repo.logout()
                  
        return doc

addHospital.execute()
doc = addHospital.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
