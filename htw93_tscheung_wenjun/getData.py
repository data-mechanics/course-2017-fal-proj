import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
import csv


class getData(dml.Algorithm):
    contributor = 'htw93_tscheung_wenjun'
    reads = []
    writes = ['htw93_tscheung_wenjun.BostonCrime', 'htw93_tscheung_wenjun.BostonHotel',
            'htw93_tscheung_wenjun.MBTAStops', 'htw93_tscheung_wenjun.BostonFood','htw93_tscheung_wenjun.BostonGarden']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')

        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BostonCrime")
        repo.createCollection("BostonCrime")
        repo['htw93_tscheung_wenjun.BostonCrime'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung_wenjun.BostonCrime')
        
        url='http://datamechanics.io/data/htw93_tscheung_wenjun/MBTA_Stops.txt'
        stops = urllib.request.urlopen(url).readlines()
        transport = []
        for stop in stops[1:]:
            s=str(stop).split(',')
            temp = {}
            temp['station']=s[2]
            temp['type'] = 'transport'
            try:
                temp['location']=[float(s[4]),float(s[5])]
                if(float(s[4])<42.230280 or float(s[4])>42.401714 or float(s[5]) < -71.185594 or float(s[5])>-70.984888):
                    continue
            except ValueError:
                continue
            transport.append(temp)
        repo.dropCollection("MBTAStops")
        repo.createCollection("MBTAStops")
        repo['htw93_tscheung_wenjun.MBTAStops'].insert_many(transport)
        #repo['htw93_tscheung.MBTAStops'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung_wenjun.MBTAStops')
        
        url = 'http://datamechanics.io/data/htw93_tscheung_wenjun/Hotel_ratings.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("BostonHotel")
        repo.createCollection("BostonHotel")
        repo['htw93_tscheung_wenjun.BostonHotel'].insert_many(r)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung_wenjun.BostonHotel')
        
        url='https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        food=json.loads(response)
        food_info=[]
        for f in food:
            try:
                temp={}
                temp['businessname']=f['businessname']
                temp['location']=f['location']['coordinates'][::-1]
                x = temp['location'][0]
                y = temp['location'][1]
                if(x<42.230280 or x>42.401714 or y < -71.185594 or y>-70.984888):
                    continue
                food_info.append(temp)
            except KeyError:
                continue
        repo.dropCollection("BostonFood")
        repo.createCollection("BostonFood")
        repo['htw93_tscheung_wenjun.BostonFood'].insert_many(food_info)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung_wenjun.BostonFood')
        
        url='https://data.cityofboston.gov/resource/rdqf-ter7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        garden=json.loads(response)
        garden_info=[]
        for f in garden:
            try:
                temp={}
                temp['site']=f['site']
                temp['location']=[float(x) for x in f['coordinates'].split(',')]
                x = temp['location'][0]
                y = temp['location'][1]
                if(x<42.230280 or x>42.401714 or y < -71.185594 or y>-70.984888):
                    continue
                garden_info.append(temp)
            except KeyError:
                continue
            except ValueError:
                continue
        repo.dropCollection("BostonGarden")
        repo.createCollection("BostonGarden")
        repo['htw93_tscheung_wenjun.BostonGarden'].insert_many(garden_info)
        #repo['htw93_tscheung.BostonCrime'].metadata({'complete':True})
        print('Finished rectrieving htw93_tscheung_wenjun.BostonGarden')


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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        
        # define entity to represent resources
        this_script = doc.agent('alg:htw93_tscheung_wenjun#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_BostonCrime = doc.entity('bdp:29yf-ye7nf', {'prov:label':'BostonCrime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_BostonFood = doc.entity('bdp:fdxy-gydq', {'prov:label':'BostonFood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_BostonGarden = doc.entity('bdp:rdqf-ter7', {'prov:label':'BostonGarden', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_MBTAStops = doc.entity('dat:htw93_tscheung_wenjun#MBTA_Stops', {'prov:label':'MBTAStops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        resource_BostonHotel = doc.entity('dat:htw93_tscheung_wenjun#Hotel_ratings', {'prov:label':'BostonHotel', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        
        # define activity to represent invocation of the script
        get_BostonCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_MBTAStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_BostonHotel =  doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_BostonFood =  doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_BostonGarden =  doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        # associate the activity with the script
        doc.wasAssociatedWith(get_BostonCrime, this_script)
        doc.wasAssociatedWith(get_MBTAStops, this_script)
        doc.wasAssociatedWith(get_BostonHotel, this_script)
        doc.wasAssociatedWith(get_BostonFood, this_script)
        doc.wasAssociatedWith(get_BostonGarden, this_script)
        
        # indicate that an activity used the entity
        doc.usage(get_BostonCrime, resource_BostonCrime, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_MBTAStops, resource_MBTAStops, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_BostonHotel, resource_BostonHotel, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_BostonFood, resource_BostonFood, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_BostonGarden, resource_BostonGarden, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        BostonCrime = doc.entity('dat:htw93_tscheung_wenjun#BostonCrime', {prov.model.PROV_LABEL:'BostonCrime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BostonCrime, this_script)
        doc.wasGeneratedBy(BostonCrime, get_BostonCrime, endTime)
        doc.wasDerivedFrom(BostonCrime, resource_BostonCrime, get_BostonCrime, get_BostonCrime, get_BostonCrime)
        
        MBTAStops = doc.entity('dat:htw93_tscheung_wenjun#MBTAStops', {prov.model.PROV_LABEL:'MBTAStops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(MBTAStops, this_script)
        doc.wasGeneratedBy(MBTAStops, get_BostonCrime, endTime)
        doc.wasDerivedFrom(MBTAStops, resource_MBTAStops, get_MBTAStops, get_MBTAStops, get_MBTAStops)
        
        BostonHotel = doc.entity('dat:htw93_tscheung_wenjun#BostonHotel', {prov.model.PROV_LABEL:'BostonHotel', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BostonHotel, this_script)
        doc.wasGeneratedBy(BostonHotel, get_BostonHotel, endTime)
        doc.wasDerivedFrom(BostonHotel, resource_BostonHotel, get_BostonHotel, get_BostonHotel, get_BostonHotel)
        
        BostonFood = doc.entity('dat:htw93_tscheung_wenjun#BostonFood', {prov.model.PROV_LABEL:'BostonFood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BostonFood, this_script)
        doc.wasGeneratedBy(BostonFood, get_BostonFood, endTime)
        doc.wasDerivedFrom(BostonFood, resource_BostonFood, get_BostonFood, get_BostonFood, get_BostonFood)
        
        BostonGarden = doc.entity('dat:htw93_tscheung_wenjun#BostonGarden', {prov.model.PROV_LABEL:'BostonGarden', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BostonGarden, this_script)
        doc.wasGeneratedBy(BostonGarden, get_BostonHotel, endTime)
        doc.wasDerivedFrom(BostonGarden, resource_BostonGarden, get_BostonGarden, get_BostonGarden, get_BostonGarden)
        
        repo.logout()
                  
        return doc

getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
