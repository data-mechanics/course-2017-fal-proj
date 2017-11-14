import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get_datasets(dml.Algorithm):
    contributor = 'jtbloom_rfballes_medinad'
    reads = []
    writes = ['jtbloom_rfballes_medinad.charging_stations', 'jtbloom_rfballes_medinad.hubway_stations', 'jtbloom_rfballes_medinad.bike_network', 'jtbloom_rfballes_medinad.neighborhoods', 'jtbloom_rfballes_medinad.tripHistory']

    @staticmethod
    def execute(trial = False):
        pass
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

        # Database 1: Electric Charging Stations
        url = 'https://opendata.arcgis.com/datasets/ed1c6fb748a646ac83b210985e1069b5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("charging_stations")
        repo.createCollection("charging_stations")
        repo['jtbloom_rfballes_medinad.charging_stations'].insert(r)
        repo['jtbloom_rfballes_medinad.charging_stations'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.charging_stations'].metadata())


        # Database 2: Hubway Station Locations
        url = 'https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/download/?format=geojson&timezone=America/New_York'
       # url = 'http://datamechanics.io/data/jtbloom_rfballes_medinad/july-hubway-station-locations.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubway_stations")
        repo.createCollection("hubway_stations")
        repo['jtbloom_rfballes_medinad.hubway_stations'].insert(r)
        repo['jtbloom_rfballes_medinad.hubway_stations'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.hubway_stations'].metadata())

        # Database 3: Existing Bike Network
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bike_network")
        repo.createCollection("bike_network")
        repo['jtbloom_rfballes_medinad.bike_network'].insert(r)
        repo['jtbloom_rfballes_medinad.bike_network'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.bike_network'].metadata())

        # Database 4: Boston Neighborhoods
        url = 'https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/download/?format=geojson&timezone=America/New_York'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
        repo['jtbloom_rfballes_medinad.neighborhoods'].insert(r)
        repo['jtbloom_rfballes_medinad.neighborhoods'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.neighborhoods'].metadata())

        # Database 5: Hubway Trip History
        #url = 'http://datamechanics.io/data/jt_rf_pr1/hubway_trip_history.json'
        url = 'http://datamechanics.io/data/jb_rfb_dm_proj2data/201708_hubway_tripdata2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("tripHistory")
        repo.createCollection("tripHistory")
        repo['jtbloom_rfballes_medinad.tripHistory'].insert(r)
        repo['jtbloom_rfballes_medinad.tripHistory'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.tripHistory'].metadata())

        # Neighborhood Income
        
        url = 'http://datamechanics.io/data/jb_rfb_dm_proj2data/incomeByNeighborhood.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("neighborhood_income")
        repo.createCollection("neighborhood_income")
        repo['jtbloom_rfballes_medinad.neighborhood_income'].insert(r)
        repo['jtbloom_rfballes_medinad.neighborhood_income'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.neighborhood_income'].metadata())


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
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('mdot','https://opendata.arcgis.com/datasets/')
        doc.add_namespace('bods', 'https://boston.opendatasoft.com/explore/dataset/')
        doc.add_namespace('ab', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('dm','http://datamechanics.io/data/jb_rfb_dm_proj2data/')
        doc.add_namespace('dmi', 'http://datamechanics.io/data/jtbloom_rfballes_medinad/')

        this_script = doc.agent('alg:get_datasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_electric = doc.entity('mdot:ed1c6fb748a646ac83b210985e1069b5_0', {'prov:label':'Electric Charging Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        #resource_hubway = doc.entity('bods:hubway-station-locations', {'prov:label':'Hubway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_hubway = doc.entity('dmi:july-hubway-station-locations', {'prov:label':'Hubway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_bike = doc.entity('ab:d02c9d2003af455fbc37f550cc53d3a4_0', {'prov:label':'Bike Network', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_neighborhoods = doc.entity('bods:boston-neighborhoods', {'prov:label':'Neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_trips = doc.entity('dm:hubway_trip_history', {'prov:label':'Trips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


        get_electric = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hubway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trips = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_electric, this_script)
        doc.wasAssociatedWith(get_hubway, this_script)
        doc.wasAssociatedWith(get_bike, this_script)
        doc.wasAssociatedWith(get_neighborhoods, this_script)
        doc.wasAssociatedWith(get_trips, this_script)

        doc.usage(get_electric, resource_electric, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        doc.usage(get_hubway, resource_hubway, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  #'ont:Query':'/download/?format=geojson&timezone=America/New_York'
                  }
                  )

        doc.usage(get_bike, resource_bike, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        doc.usage(get_neighborhoods, resource_neighborhoods, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'/download/?format=geojson&timezone=America/New_York'
                  }
                  )

        doc.usage(get_trips, resource_trips, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        electric = doc.entity('dat:jtbloom_rfballes_medinad#electric', {prov.model.PROV_LABEL:'Electric Charging Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(electric, this_script)
        doc.wasGeneratedBy(electric, get_electric, endTime)
        doc.wasDerivedFrom(electric, resource_electric, get_electric, get_electric, get_electric)

        hubway = doc.entity('dat:jtbloom_rfballes_medinad#hubway', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_hubway, endTime)
        doc.wasDerivedFrom(hubway, resource_hubway, get_hubway, get_hubway, get_hubway)

        bike = doc.entity('dat:jtbloom_rfballes_medinad#bike', {prov.model.PROV_LABEL:'Bike Network', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bike, this_script)
        doc.wasGeneratedBy(bike, get_bike, endTime)
        doc.wasDerivedFrom(bike, resource_bike, get_bike, get_bike, get_bike)

        neighborhoods = doc.entity('dat:jtbloom_rfballes_medinad#neighborhoods', {prov.model.PROV_LABEL:'Boston Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, get_neighborhoods, endTime)
        doc.wasDerivedFrom(neighborhoods, resource_neighborhoods, get_neighborhoods, get_neighborhoods, get_neighborhoods)

        trips = doc.entity('dat:jtbloom_rfballes_medinad#trips', {prov.model.PROV_LABEL:'Trips', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trips, this_script)
        doc.wasGeneratedBy(trips, get_trips, endTime)
        doc.wasDerivedFrom(trips, resource_trips, get_trips, get_trips, get_trips)


        repo.logout()
                  
        return doc

get_datasets.execute()
#doc = get_datasets.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof