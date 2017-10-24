import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty

class SchoolsHospitalsCombo(dml.Algorithm):
    contributor = 'htw93_tscheung'
    reads = ['htw93_tscheung.BostonSchools', 'htw93_tscheung.NewYorkCitySchools','htw93_tscheung.BostonHospitals', 'htw93_tscheung.NewYorkCityHospitals']
    writes = ['htw93_tscheung.BostonSchoolsHospitals', 'htw93_tscheung.NewYorkCitySchoolsHospitals']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung', 'htw93_tscheung')

        bostonHospitals = repo.htw93_tscheung.BostonHospitals
        newYorkCityHospitals = repo.htw93_tscheung.NewYorkCityHospitals
        bostonSchools = repo.htw93_tscheung.BostonSchools
        newYorkCitySchools = repo.htw93_tscheung.NewYorkCitySchools

        bosHospitals = bostonHospitals.find()
        bosSchools = bostonSchools.find()
        nycHospitals = newYorkCityHospitals.find()
        nycSchools = newYorkCitySchools.find()
        bostonSchoolsHospitals = []
        newYorkCitySchoolsHospitals = []
        
        # Combine boston hospitals and boston schools.
        for h in bosHospitals:
            for s in bosSchools:
                hLoc = (float(h['location']['latitude']),float(h['location']['longitude']))
                sLoc = (float(s['properties']['Latitude']),float(s['properties']['Longitude']))
                dis = vincenty(hLoc,sLoc,miles=True)
                if dis < 1.5:
                    bostonSchoolsHospitals.append({'school':s['properties']['Name'],'hospital':h['name'],'distance':dis})
            bosSchools.rewind()

        repo.dropCollection("BostonSchoolsHospitals")
        repo.createCollection("BostonSchoolsHospitals")
        repo['htw93_tscheung.BostonSchoolsHospitals'].insert_many(bostonSchoolsHospitals)
        print('Finished creating collection htw93_tscheung.BostonSchoolsHospitals')
        
        # Combine nyc hospitals and nyc schools.
        for h in nycHospitals:
            for s in nycSchools:
                hLoc = (float(h['location_1']['latitude']),float(h['location_1']['longitude']))
                sLoc = (float(s['the_geom']['coordinates'][1]),float(s['the_geom']['coordinates'][0]))
                dis = vincenty(hLoc,sLoc,miles=True)
                if dis < 1.5:
                    newYorkCitySchoolsHospitals.append({'school':s['name'],'hospital':h['facility_name'],'distance':dis})

            nycSchools.rewind()

        repo.dropCollection("NewYorkCitySchoolsHospitals")
        repo.createCollection("NewYorkCitySchoolsHospitals")
        repo['htw93_tscheung.NewYorkCitySchoolsHospitals'].insert_many(newYorkCitySchoolsHospitals)
        print('Finished creating collection htw93_tscheung.NewYorkCitySchoolsHospitals')

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
        
        SchoolHospitalsCombo_script = doc.agent('alg:htw93_tscheung#SchoolHospitalsCombo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        BostonSchools = doc.entity('dat:htw93_tscheung#BostonSchools', {'prov:label':'Boston Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        BostonHospitals = doc.entity('dat:htw93_tscheung#BostonHospitals', {'prov:label':'Boston Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        BostonSchoolsHospitals = doc.entity('dat:BostonSchoolsHospitals', {'prov:label':'Boston Schools Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCitySchools = doc.entity('dat:NewYorkCitySchools', {'prov:label':'New York City Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCityHospitals = doc.entity('dat:NewYorkCityHospitals', {'prov:label':'New York City Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        NewYorkCitySchoolsHospitals = doc.entity('dat:NewYorkCitySchoolsHospitals', {'prov:label':'New York City Schools Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_BostonSchoolsHospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_NewYorkCitySchoolsHospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_BostonSchoolsHospitals, SchoolHospitalsCombo_script)
        doc.wasAssociatedWith(get_NewYorkCitySchoolsHospitals, SchoolHospitalsCombo_script)
        
        doc.wasAttributedTo(BostonSchoolsHospitals, SchoolHospitalsCombo_script)
        doc.wasAttributedTo(NewYorkCitySchoolsHospitals, SchoolHospitalsCombo_script)

        doc.wasGeneratedBy(BostonSchoolsHospitals, get_BostonSchoolsHospitals, endTime)
        doc.wasGeneratedBy(NewYorkCitySchoolsHospitals,get_BostonSchoolsHospitals, endTime)

        doc.wasDerivedFrom(BostonSchools, BostonHospitals, get_BostonSchoolsHospitals, get_BostonSchoolsHospitals, get_BostonSchoolsHospitals)
        doc.wasDerivedFrom(NewYorkCitySchools, NewYorkCityHospitals, get_NewYorkCitySchoolsHospitals, get_NewYorkCitySchoolsHospitals, get_NewYorkCitySchoolsHospitals)

        repo.logout()
                  
        return doc

SchoolsHospitalsCombo.execute()
doc = SchoolsHospitalsCombo.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
