import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty

class SchoolsCrimeCombo(dml.Algorithm):
    contributor = 'htw93_tscheung'
    reads = ['htw93_tscheung.BostonCrime', 'htw93_tscheung.NewYorkCityCrime','htw93_tscheung.BostonSchools', 'htw93_tscheung.NewYorkCitySchools']
    writes = ['htw93_tscheung.BostonSchoolsCrime', 'htw93_tscheung.NewYorkCitySchoolsCrime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung', 'htw93_tscheung')

        bostonCrime = repo.htw93_tscheung.BostonCrime
        newYorkCityCrime = repo.htw93_tscheung.NewYorkCityCrime
        bostonSchools = repo.htw93_tscheung.BostonSchools
        newYorkCitySchools = repo.htw93_tscheung.NewYorkCitySchools

        bosCrime = bostonCrime.find()
        bosSchools = bostonSchools.find()
        nycCrime = newYorkCityCrime.find()
        nycSchools = newYorkCitySchools.find()
        bostonSchoolsCrime = []
        newYorkCitySchoolsCrime = []
        
        # Combine boston crime and boston schools.
        for c in bosCrime:
            for s in bosSchools:
                if 'lat' in c and 'long' in c:
                    cLoc = (float(c['lat']),float(c['long']))
                    sLoc = (float(s['properties']['Latitude']),float(s['properties']['Longitude']))
                    dis = vincenty(cLoc,sLoc,miles=True)
                    if dis < 0.5:
                        bostonSchoolsCrime.append({'school':s['properties']['Name'],'crime_description':c['offense_description'],'distance':dis,'day_of_week':c['day_of_week'],'hour':c['hour']})
            bosSchools.rewind()

        repo.dropCollection("BostonSchoolsCrime")
        repo.createCollection("BostonSchoolsCrime")
        repo['htw93_tscheung.BostonSchoolsCrime'].insert_many(bostonSchoolsCrime)
        print('Finished creating collection htw93_tscheung.BostonSchoolsCrime')
        
        # Combine nyc crime and nyc schools.
        for c in nycCrime:
            for s in nycSchools:
                if 'latitude' in c and 'longitude' in c:
                    cLoc = (float(c['latitude']),float(c['longitude']))
                    sLoc = (float(s['the_geom']['coordinates'][1]),float(s['the_geom']['coordinates'][0]))
                    dis = vincenty(cLoc,sLoc,miles=True)
                    if dis < 1:
                        newYorkCitySchoolsCrime.append({'school':s['name'],'crime_description':c['ofns_desc'],'distance':dis,'time':c['cmplnt_fr_tm']})

            nycSchools.rewind()

        repo.dropCollection("NewYorkCitySchoolsCrime")
        repo.createCollection("NewYorkCitySchoolsCrime")
        repo['htw93_tscheung.NewYorkCitySchoolsCrime'].insert_many(newYorkCitySchoolsCrime)
        print('Finished creating collection htw93_tscheung.NewYorkCitySchoolsCrime')

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
        repo.authenticate('htw93_tscheung', 'htw93_tscheung')
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

SchoolsCrimeCombo.execute()
doc = SchoolsCrimeCombo.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
