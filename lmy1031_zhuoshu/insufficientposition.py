import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict

from collections import Counter

class findposition(dml.Algorithm):
    contributor = 'lmy1031_zhuoshu'
    reads = ['lmy1031_zhuoshu.insufficient', 'lmy1031_zhuoshu.location']
    writes = ['lmy1031_zhuoshu.position']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Start connecting database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')  
        insufficient = repo.lmy1031_zhuoshu.insufficient.find()
        location = repo.lmy1031_zhuoshu.location.find()
        #make the cursor to dict
        y   = []
        
        for i in insufficient:
            y.append(i)
    
        insufficient=y[0]
        #delete useless key
        insufficient.pop('_id', None)
        print(insufficient)
        
       
        
        x   = []
        
        for i in location:
            x.append(i)
        x=x[0]
        #delete the useless key
        x.pop('_id', None)
        x.pop('0Zip', None)
        
        
        list=[]
        #iterate location and emergency several times to find the ratio of hospital/garden in specific zone
        for i in insufficient:
            for j in x:
                if (i==j):
                    list+=[x[j]]
        print(list)
        temp={}
        temp["location"]=list
    
        repo.dropCollection("position")
        repo.createCollection("position")
        repo['lmy1031_zhuoshu.position'].insert(temp)
                    
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
        
        location = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.location', {prov.model.PROV_LABEL:'location', prov.model.PROV_TYPE:'ont:DataSet'})
        emergency1= doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu.emergency', {prov.model.PROV_LABEL:'emergnecy', prov.model.PROV_TYPE:'ont:DataSet'})

        findposition = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(findposition, this_script)
        
        doc.usage(findposition, location, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(findposition, emergency1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        
        insufficient= doc.entity('dat:#insufficient',
                                    {prov.model.PROV_LABEL:'insufficient',
                                     prov.model.PROV_TYPE:'ont:DataSet'})

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(insufficient, this_script)
        doc.wasGeneratedBy(insufficient, findposition, endTime)
        doc.wasDerivedFrom(insufficient, location, findposition, findposition, findposition)
        doc.wasDerivedFrom(insufficient, emergency1, findposition, findposition, findposition)
        


        repo.logout()
                  
        return doc

findposition.execute()
doc = findposition.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
