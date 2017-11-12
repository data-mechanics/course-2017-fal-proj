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
    reads = ['htw93_tscheung_wenjun.BostonCrime', 'htw93_tscheung_wenjun.BostonHotel','htw93_tscheung_wenjun.MBTAStops']
    writes = ['htw93_tscheung_wenjun.BostonHotelData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')

        BostonCrime = repo.htw93_tscheung_wenjun.BostonCrime
        BostonHotel = repo.htw93_tscheung_wenjun.BostonHotel
        MBTAStops = repo.htw93_tscheung_wenjun.MBTAStops

        BosCrime = BostonCrime.find()
        BosHotel = BostonHotel.find()
        MBTA = MBTAStops.find()

        BostonHotelData = []
        
        # Combine boston crime and boston schools.
        for h in BosHotel:
            count_crime = 0
            count_mbta = 0
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
            BostonHotelData.append({'hotel':h['Hotel_name'],'crime_count':count_crime,'mbta_count':count_mbta})
            BosCrime.rewind()
            MBTA.rewind()
            

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('ndp', 'https://data.cityofnewyork.us/resource/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        
        SchoolCrimeCombo_script = doc.agent('alg:htw93_tscheung#SchoolCrimeCombo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        BostonSchools = doc.entity('dat:htw93_tscheung#BostonSchools', {'prov:label':'Boston Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        BostonCrime = doc.entity('dat:htw93_tscheung#BostonCrime', {'prov:label':'Boston Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        BostonSchoolsCrime = doc.entity('dat:BostonSchoolsCrime', {'prov:label':'Boston Schools Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCitySchools = doc.entity('dat:NewYorkCitySchools', {'prov:label':'New York City Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCityCrime = doc.entity('dat:NewYorkCityCrime', {'prov:label':'New York City Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCitySchoolsCrime = doc.entity('dat:NewYorkCitySchoolsCrime', {'prov:label':'New York City Schools Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_BostonSchoolsCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_NewYorkCitySchoolsCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_BostonSchoolsCrime, SchoolCrimeCombo_script)
        doc.wasAssociatedWith(get_NewYorkCitySchoolsCrime, SchoolCrimeCombo_script)
        
        doc.wasAttributedTo(BostonSchoolsCrime, SchoolCrimeCombo_script)
        doc.wasAttributedTo(NewYorkCitySchoolsCrime, SchoolCrimeCombo_script)

        doc.wasGeneratedBy(BostonSchoolsCrime, get_BostonSchoolsCrime, endTime)
        doc.wasGeneratedBy(NewYorkCitySchoolsCrime,get_BostonSchoolsCrime, endTime)

        doc.wasDerivedFrom(BostonSchools, BostonCrime, get_BostonSchoolsCrime, get_BostonSchoolsCrime, get_BostonSchoolsCrime)
        doc.wasDerivedFrom(NewYorkCitySchools, NewYorkCityCrime, get_NewYorkCitySchoolsCrime, get_NewYorkCitySchoolsCrime, get_NewYorkCitySchoolsCrime)

        repo.logout()
                  
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof