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
    contributor = 'htw93_tscheung'
    reads = ['htw93_tscheung.BostonCrime', 'htw93_tscheung.NewYorkCityCrime','htw93_tscheung.BostonSchools', 'htw93_tscheung.NewYorkCitySchools']
    writes = ['htw93_tscheung.BostonSchoolCrime', 'htw93_tscheung.NewYorkCitySchoolCrime']

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
        bosSchool = bostonSchools.find()
        nycCrime = newYorkCityCrime.find()
        nycSchool = newYorkCitySchools.find()
        bostonSchoolCrime = []
        newYorkCitySchoolCrime = []
        
        # Combine boston crime and boston schools.
        count = 0
        for c in bosCrime:
            for s in bosSchool:
                count += 1
                if 'lat' in c and 'long' in c:
                    cLoc = (float(c['lat']),float(c['long']))
                    sLoc = (float(s['properties']['Latitude']),float(s['properties']['Longitude']))
                    dis = vincenty(cLoc,sLoc,miles=True)
                    if dis < 0.5:
                        bostonSchoolCrime.append({'school':s['properties']['Name'],'crime_description':c['offense_description'],'distance':dis,'day_of_week':c['day_of_week'],'hour':c['hour']})
            bosSchool.rewind()
        print(count)

        repo.dropCollection("BostonSchoolCrime")
        repo.createCollection("BostonSchoolCrime")
        repo['htw93_tscheung.BostonSchoolCrime'].insert_many(bostonSchoolCrime)
        print('Finished creating collection htw93_tscheung.BostonSchoolCrime')
        
        # Combine nyc crime and nyc schools.
        count = 0
        for c in nycCrime:
            for s in nycSchool:
                count += 1
                if 'latitude' in c and 'longitude' in c:
                    cLoc = (float(c['latitude']),float(c['longitude']))
                    sLoc = (float(s['the_geom']['coordinates'][1]),float(s['the_geom']['coordinates'][0]))
                    dis = vincenty(cLoc,sLoc,miles=True)
                    if dis < 1:
                        newYorkCitySchoolCrime.append({'school':s['name'],'crime_description':c['ofns_desc'],'distance':dis,'time':c['cmplnt_fr_tm']})

            nycSchool.rewind()
        print(count)

        repo.dropCollection("NewYorkCitySchoolCrime")
        repo.createCollection("NewYorkCitySchoolCrime")
        repo['htw93_tscheung.NewYorkCitySchoolCrime'].insert_many(newYorkCitySchoolCrime)
        print('Finished creating collection htw93_tscheung.NewYorkCitySchoolCrime')

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

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
