import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty

class transformation1(dml.Algorithm):
    contributor = 'htw93_tscheung_wenjun'
    reads = ['htw93_tscheung_wenjun.BostonCrime', 'htw93_tscheung_wenjun.BostonHotel','htw93_tscheung_wenjun.MBTAStops','htw93_tscheung_wenjun.BostonFood','htw93_tscheung_wenjun.BostonGarden']
    writes = ['htw93_tscheung_wenjun.BostonHotelData']

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')

        BostonCrime = repo.htw93_tscheung_wenjun.BostonCrime
        BostonHotel = repo.htw93_tscheung_wenjun.BostonHotel
        MBTAStops = repo.htw93_tscheung_wenjun.MBTAStops
        BostonFood = repo.htw93_tscheung_wenjun.BostonFood
        BostonGarden = repo.htw93_tscheung_wenjun.BostonGarden

        BosCrime = BostonCrime.find()
        BosHotel = BostonHotel.find()
        MBTA = MBTAStops.find()
        BosFood = BostonFood.find()
        BosGarden = BostonGarden.find()

        BostonHotelData = []
        
        for h in BosHotel:
            count_crime = 0
            count_mbta = 0
            count_food = 0
            count_garden = 0
            hLoc = (float(h['lat']),float(h['lon']))
            for c in BosCrime:
                if 'lat' in c and 'long' in c:
                    cLoc = (float(c['lat']),float(c['long']))
                    dis = vincenty(cLoc,hLoc,miles=True)
                    if dis < 0.5:
                        count_crime+=1
            for m in MBTA:
                mLoc = (float(m['location'][0]),float(m['location'][1]))
                dis = vincenty(mLoc,hLoc,miles=True)
                if dis < 0.5:
                    count_mbta+=1
            for f in BosFood:
                if f['location'][0] != 0 and f['location'][1] != 0:
                    fLoc =(float(f['location'][0]),float(f['location'][1]))
                    dis = vincenty(fLoc,hLoc,miles=True)
                    if dis < 0.2:
                        count_food+=1
            for g in BosGarden:
                if g['location'][0] != 0 and g['location'][1] != 0:
                    gLoc =(float(g['location'][0]),float(g['location'][1]))
                    dis = vincenty(gLoc,hLoc,miles=True)
                    if dis < 1:
                        count_garden+=1  
                               
            BostonHotelData.append({'hotel':h['Hotel_name'],'rate':h['Avg_rate'],'crime_count':count_crime,'mbta_count':count_mbta,'food_count':count_food,'garden_count':count_garden})
            BosCrime.rewind()
            MBTA.rewind()
            BosFood.rewind()
            BosGarden.rewind()
            

        repo.dropCollection("BostonHotelData")
        repo.createCollection("BostonHotelData")
        repo['htw93_tscheung_wenjun.BostonHotelData'].insert_many(BostonHotelData)
        print('Finished creating collection htw93_tscheung_wenjun.BostonHotelData')
        
        

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
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:htw93_tscheung_wenjun#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_BostonCrime = doc.entity('dat:htw93_tscheung_wenjun#BostonCrime', {'prov:label':'BostonCrime', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_MBTAStops = doc.entity('dat:htw93_tscheung_wenjun#MBTAStops', {'prov:label':'MBTAStops', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_BostonHotel = doc.entity('dat:htw93_tscheung_wenjun#BostonHotel', {'prov:label':'BostonHotel', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_BostonFood = doc.entity('dat:htw93_tscheung_wenjun#BostonFood', {'prov:label':'BostonFood', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_BostonGarden = doc.entity('dat:htw93_tscheung_wenjun#BostonGarden', {'prov:label':'BostonGarden', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        
        # define activity to represent invocation of the script
        get_BostonHotelData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # asscoiate the activity with the script
        doc.wasAssociatedWith(get_BostonHotelData, this_script)
        # indicate that an activity used the entity
        doc.usage(get_BostonHotelData, resource_BostonCrime, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_BostonHotelData, resource_MBTAStops, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_BostonHotelData, resource_BostonHotel, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_BostonHotelData, resource_BostonFood, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_BostonHotelData, resource_BostonGarden, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        
        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        BostonHotelData = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelData', {prov.model.PROV_LABEL: 'BostonHotelData', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(BostonHotelData, this_script)
        doc.wasGeneratedBy(BostonHotelData, get_BostonHotelData, endTime)
        doc.wasDerivedFrom(BostonHotelData, resource_BostonCrime, get_BostonHotelData, get_BostonHotelData, get_BostonHotelData)
        doc.wasDerivedFrom(BostonHotelData, resource_MBTAStops, get_BostonHotelData, get_BostonHotelData, get_BostonHotelData)
        doc.wasDerivedFrom(BostonHotelData, resource_BostonHotel, get_BostonHotelData, get_BostonHotelData, get_BostonHotelData)
        doc.wasDerivedFrom(BostonHotelData, resource_BostonFood, get_BostonHotelData, get_BostonHotelData, get_BostonHotelData)
        doc.wasDerivedFrom(BostonHotelData, resource_BostonGarden, get_BostonHotelData, get_BostonHotelData, get_BostonHotelData)



        repo.logout()
                  
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
