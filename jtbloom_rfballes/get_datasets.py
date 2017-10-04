import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get_datasets(dml.Algorithm):
    contributor = 'jtbloom_rfballes'
    reads = []
    writes = ['jtbloom_rfballes.charging_stations', 'jtbloom_rfballes.hubway_stations', 'jtbloom_rfballes.bike_network']

    @staticmethod
    def execute(trial = False):
        pass
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes', 'jtbloom_rfballes')

        # Database 1: Electric Charging Stations
        url = 'https://opendata.arcgis.com/datasets/ed1c6fb748a646ac83b210985e1069b5_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("charging_stations")
        repo.createCollection("charging_stations")
        repo['jtbloom_rfballes.charging_stations'].insert(r)
        repo['jtbloom_rfballes.charging_stations'].metadata({'complete':True})
        print(repo['jtbloom_rfballes.charging_stations'].metadata())


        # Database 2: Hubway Station Locations
        url = 'https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/download/?format=geojson&timezone=America/New_York'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hubway_stations")
        repo.createCollection("hubway_stations")
        repo['jtbloom_rfballes.hubway_stations'].insert(r)
        repo['jtbloom_rfballes.hubway_stations'].metadata({'complete':True})
        print(repo['jtbloom_rfballes.hubway_stations'].metadata())

        # Database 3: Existing Bike Network
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bike_network")
        repo.createCollection("bike_network")
        repo['jtbloom_rfballes.bike_network'].insert(r)
        repo['jtbloom_rfballes.bike_network'].metadata({'complete':True})
        print(repo['jtbloom_rfballes.bike_network'].metadata())

        # Database 4: Boston Neighborhoods
        url = 'https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/download/?format=geojson&timezone=America/New_York'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
        repo['jtbloom_rfballes.neighborhoods'].insert(r)
        repo['jtbloom_rfballes.neighborhoods'].metadata({'complete':True})
        print(repo['jtbloom_rfballes.neighborhoods'].metadata())

        # Database 5: Trees
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/ce863d38db284efe83555caf8a832e2a_1.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trees")
        repo.createCollection("trees")
        repo['jtbloom_rfballes.trees'].insert(r)
        repo['jtbloom_rfballes.trees'].metadata({'complete':True})
        print(repo['jtbloom_rfballes.trees'].metadata())


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
        repo.authenticate('jtbloom_rfballes', 'jtbloom_rfballes')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('mdot','https://opendata.arcgis.com/datasets/')
        doc.add_namespace('bods', 'https://boston.opendatasoft.com/explore/dataset/')
        doc.add_namespace('ab', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
       # doc.add_namespace('abos','https://opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:get_datasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_electric = doc.entity('mdot:ed1c6fb748a646ac83b210985e1069b5_0', {'prov:label':'Electric Charging Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_hubway = doc.entity('bods:hubway-station-locations', {'prov:label':'Hubway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_bike = doc.entity('ab:d02c9d2003af455fbc37f550cc53d3a4_0', {'prov:label':'Bike Network', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_neighborhoods = doc.entity('bods:boston-neighborhoods', {'prov:label':'Neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_trees = doc.entity('ab:ce863d38db284efe83555caf8a832e2a_1', {'prov:label':'Trees', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})


        get_electric = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hubway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trees = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_electric, this_script)
        doc.wasAssociatedWith(get_hubway, this_script)
        doc.wasAssociatedWith(get_bike, this_script)
        doc.wasAssociatedWith(get_neighborhoods, this_script)
        doc.wasAssociatedWith(get_trees, this_script)

        doc.usage(get_electric, resource_electric, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        doc.usage(get_hubway, resource_hubway, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'/download/?format=geojson&timezone=America/New_York'
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

        doc.usage(get_trees, resource_trees, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        electric = doc.entity('dat:jtbloom_rfballes#electric', {prov.model.PROV_LABEL:'Electric Charging Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(electric, this_script)
        doc.wasGeneratedBy(electric, get_electric, endTime)
        doc.wasDerivedFrom(electric, resource_electric, get_electric, get_electric, get_electric)

        hubway = doc.entity('dat:jtbloom_rfballes#hubway', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_hubway, endTime)
        doc.wasDerivedFrom(hubway, resource_hubway, get_hubway, get_hubway, get_hubway)

        bike = doc.entity('dat:jtbloom_rfballes#bike', {prov.model.PROV_LABEL:'Bike Network', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bike, this_script)
        doc.wasGeneratedBy(bike, get_bike, endTime)
        doc.wasDerivedFrom(bike, resource_bike, get_bike, get_bike, get_bike)

        neighborhoods = doc.entity('dat:jtbloom_rfballes#neighborhoods', {prov.model.PROV_LABEL:'Boston Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, get_neighborhoods, endTime)
        doc.wasDerivedFrom(neighborhoods, resource_neighborhoods, get_neighborhoods, get_neighborhoods, get_neighborhoods)

        trees = doc.entity('dat:jtbloom_rfballes#trees', {prov.model.PROV_LABEL:'Trees', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trees, this_script)
        doc.wasGeneratedBy(trees, get_trees, endTime)
        doc.wasDerivedFrom(trees, resource_trees, get_trees, get_trees, get_trees)


        repo.logout()
                  
        return doc

#example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
