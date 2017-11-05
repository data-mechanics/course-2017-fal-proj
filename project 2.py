import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests


class fzjr_retrievalalgorithm(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    reads = []
    writes = ['francisz_jrashaan.hubways', 'francisz_jrashaan.ChargingStation', 'francisz_jrashaan.bikeNetwork',
              'francisz_jrashaan.openspace', 'francisz_jrashaan.neighborhood']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
        repo.dropCollection("hubways")
        repo.createCollection("hubways")
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        
      
        repo['francisz_jrashaan.hubways'].insert_many(geoList)
        repo['francisz_jrashaan.hubways'].metadata({'complete':True})
        # print(repo['francisz_jrashaan.crime'].metadata())

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/465e00f9632145a1ad645a27d27069b4_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
       
        repo.dropCollection("ChargingStation")
        repo.createCollection("ChargingStation")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['francisz_jrashaan.ChargingStation'].insert_many(geoList)
        repo['francisz_jrashaan.ChargingStation'].metadata({'complete':True})


        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        repo.dropCollection("bikeNetwork")
        repo.createCollection("bikeNetwork")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['francisz_jrashaan.bikeNetworks'].insert_many(geoList)
        repo['francisz_jrashaan.bikeNetworks'].metadata({'complete':True})


        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
       
        repo.dropCollection("neighborhood")
        repo.createCollection("neighborhood")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo['francisz_jrashaan.neighborhood'].insert(geoList)
        repo['francisz_jrashaan.neighborhood'].metadata({'complete':True})

        # repo['francisz_jrashaan.capopulation'].metadata({'complete':True})
        # print(repo['francisz_jrashaan.capopulation'].metadata())
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        geoDict = dict(gj)
        geoList = geoDict['features']
        repo.dropCollection("openspace")
        repo.createCollection("openspace")
        repo['francisz_jrashaan.openspace'].insert_many(geoList)
        repo['francisz_jrashaan.openspace'].insert_many({'complete': True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        #doc.add_namespace('cam', 'https://data.cambridgema.gov/api/views/')

        this_script = doc.agent('alg:francisz_jrashaan#fzjr_retrievalalgorithm',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_hubways = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b',
                                    {'prov:label': 'hubways', prov.model.PROV_TYPE: 'ont:DataResource',
                                     'ont:Extension': 'geojson'})
        resource_ChargingStation = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5',
                                           {'prov:label': 'ChargingStation', prov.model.PROV_TYPE: 'ont:DataResource',
                                            'ont:Extension': 'geojson'})
        resource_bikeNetworks = doc.entity('cam:srp4-fhjz/rows.json',
                                      {'prov:label': 'bikeNetworks', prov.model.PROV_TYPE: 'ont:DataResource',
                                       'ont:Extension': 'geojson'})
        resource_capopulation = doc.entity('cam:r4pm-qqje/rows', {'prov:label': 'Cambridge Population Density',
                                                                  prov.model.PROV_TYPE: 'ont:DataResource',
                                                                  'ont:Extension': 'json'})
        resource_openspace = doc.entity('bdp:769c0a21-9e35-48de-a7b0-2b7dfdefd35e',
                                        {'prov:label': 'Openspace', prov.model.PROV_TYPE: 'ont:DataResource',
                                         'ont:Extension': 'geojson'})

        get_hubways = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_ChargingStation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_bikeNetworks = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_capopulation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_openspace = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubway, this_script)
        doc.wasAssociatedWith(get_ChargingStation, this_script)
        doc.wasAssociatedWith(get_bikeNetworks, this_script)
        doc.wasAssociatedWith(get_capopulation, this_script)
        doc.wasAssociatedWith(get_openspace, this_script)
        doc.usage(get_hubways, resource_hubways, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'}
                  )
        doc.usage(get_ChargingStation, resource_ChargingStation, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        doc.usage(get_bikeNetworks, resource_bikeNetworks, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'}
                  )
        doc.usage(get_capopulation, resource_capopulation, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'}
                  )
        doc.usage(get_openspace, resource_openspace, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        hubways = doc.entity('dat:francisz_jrashaan#hubways',
                           {prov.model.PROV_LABEL: 'hubways', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_hubways, endTime)
        doc.wasDerivedFrom(crime, resource_hubways, get_hubways, get_hubways, get_hubways)

        ChargingStation = doc.entity('dat:francisz_jrashaan#ChargingStations',
                                  {prov.model.PROV_LABEL: 'ChargingStations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(streetlights, this_script)
        doc.wasGeneratedBy(streetlights, get_ChargingStation, endTime)
        doc.wasDerivedFrom(streetlights, resource_ChargingStation, get_ChargingStation, get_ChargingStation, get_ChargingStation)

        bikeNetworks = doc.entity('dat:francisz_jrashaan#bikeNetworks',
                             {prov.model.PROV_LABEL: 'bikeNetworks', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(bikeNetworks, this_script)
        doc.wasGeneratedBy(bikeNetworks, get_bikeNetworks, endTime)
        doc.wasDerivedFrom(bikeNetworks, resource_bikeNetworks, get_bikeNetworks, get_bikeNetworks, get_bikeNetworks)

        capopulation = doc.entity('dat:francisz_jrashaan#capopulation',
                                  {prov.model.PROV_LABEL: 'capopulation', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(capopulation, this_script)
        doc.wasGeneratedBy(capopulation, get_capopulation, endTime)
        doc.wasDerivedFrom(capopulation, resource_capopulation, get_capopulation, get_capopulation, get_capopulation)

        openspace = doc.entity('dat:francisz_jrashaan#openspace',
                               {prov.model.PROV_LABEL: 'openspace', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(openspace, this_script)
        doc.wasGeneratedBy(openspace, get_openspace, endTime)
        doc.wasDerivedFrom(openspace, resource_openspace, get_openspace, get_openspace, get_openspace)

        repo.logout()

        return doc


fzjr_retrievalalgorithm.execute()
doc = fzjr_retrievalalgorithm.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
