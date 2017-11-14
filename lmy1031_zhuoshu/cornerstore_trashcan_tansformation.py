import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from collections import Counter

def helper(lat1, lon1, lat2, lon2):
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)

        dlon = lon1 - lon2

        EARTH_R = 6372.8

        y = sqrt(
            (cos(lat2) * sin(dlon)) ** 2
            + (cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)) ** 2
            )
        x = sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(dlon)
        c = atan2(y, x)
        return EARTH_R * c

class cornerstore_trashcan_transformation(dml.Algorithm):
    contributor = 'lmy1031_zhuoshu'
    reads = ['lmy1031_zhuoshu_cornerstore', 'lmy1031_zhuoshu_trashcan']
    writes = ['lmy1031_zhuoshu_recycle']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Start connecting database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')  
        bostonCornerstore = repo.lmy1031_zhuoshu_cornerstore.find()
        bostonTrashcan = repo.lmy1031_zhuoshu_trashcan.find()
      
        repo.dropCollection("recycle")
        repo.createCollection("recycle")

        print("start cornerstore_trashcan transformation!")

        #preprocess_store = []

        for i in bostonCornerstore:
            i.pop(":@computed_region_aywg_kpfh")
        

        location_trashcan = []
        for i in bostonTrashcan:
            if "Location" in i:
                location_trashcan.append(i["Location"])

        sets=[]
        for i in location_trashcan:
            if i!='':
                longtitude_trashcan=''
                longt=''
                for j in range((len(i)-1)):
                    if j >= 1 and i[j] != ',':
                        if i[j]!=' ' :
                            longtitude_trashcan += i[j]
                    elif i[j] == ',':
                    
                        longt=float(longtitude_trashcan)
                        longtitude_trashcan=''
                    
                    
                sets.append((float(longtitude_trashcan),longt))





        print(location_trashcan)
        
                    
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

        this_script = doc.agent('alg:#cornerstore_trashcan_transformation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        boston_hospital = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu_cornerstore', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        boston_garden = doc.entity('dat:lmy1031_zhuoshu#lmy1031_zhuoshu_trashcan', {prov.model.PROV_LABEL:'garden', prov.model.PROV_TYPE:'ont:DataSet'})

        boston_garden_hospital_emergency = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(boston_garden_hospital_emergency, this_script)
        
        doc.usage(boston_garden_hospital_emergency, boston_hospital, startTime, None, 
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(boston_garden_hospital_emergency, boston_garden, startTime, None, 
                  {prov.model.PROV_TYPE:'ont:Computation'})
        
        boston_emergency = doc.entity('dat:#emergency',
                                    {prov.model.PROV_LABEL:'emergency',
                                     prov.model.PROV_TYPE:'ont:DataSet'})

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_emergency, this_script)
        doc.wasGeneratedBy(boston_emergency, boston_garden_hospital_emergency, endTime)
        doc.wasDerivedFrom(boston_emergency, boston_hospital, boston_garden_hospital_emergency, boston_garden_hospital_emergency, boston_garden_hospital_emergency)
        doc.wasDerivedFrom(boston_emergency, boston_garden, boston_garden_hospital_emergency, boston_garden_hospital_emergency, boston_garden_hospital_emergency)


        repo.logout()
                  
        return doc

cornerstore_trashcan_transformation.execute()
doc = cornerstore_trashcan_transformation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))