import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class projOne(dml.Algorithm):
    contributor = 'klovett'
    reads = []
    writes = ['klovett.bbLocations', 'klovett.bbAlerts', 'klovett.treeLocations', 'klovett.trashPickup','klovett.landCambridge', 'klovett.landBoston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('klovett', 'klovett')
        
        
        url = 'https://data.boston.gov/export/15e/7fa/15e7fa44-b9a8-42da-82e1-304e43460095.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bbLocations")
        repo.createCollection("bbLocations")
        repo['klovett.bbLocations'].insert_many(r)
        repo['klovett.bbLocations'].metadata({'complete':True})
        print(repo['klovett.bbLocations'].metadata())
        
        url = 'https://data.boston.gov/export/c8c/54c/c8c54c49-3097-40fc-b3f2-c9508b8d393a.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace("]", "")
        response = response + "]"
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bbAlerts")
        repo.createCollection("bbAlerts")
        repo['klovett.bbAlerts'].insert_many(r)
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/ce863d38db284efe83555caf8a832e2a_1.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        response = str(r["features"])
        response = response.replace("\'", "\"")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("treeLocations")
        repo.createCollection("treeLocations")
        repo['klovett.treeLocations'].insert_many(r)

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/b09b9dd54c1241369080c0ee48895e85_10.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        response = str(r["features"])
        response = response.replace("\'", "\"")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trashPickup")
        repo.createCollection("trashPickup")
        repo['klovett.trashPickup'].insert_many(r)
        
        url = 'https://data.cambridgema.gov/resource/ufnx-m9uc.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("landCambridge")
        repo.createCollection("landCambridge")
        repo['klovett.landCambridge'].insert_many(r)

        url = 'https://data.boston.gov/export/5b0/274/5b027436-5213-4be6-ab5f-485a03f74500.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace("]", "")
        response = response + "]"
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("landBoston")
        repo.createCollection("landBoston")
        repo['klovett.landBoston'].insert_many(r)
        
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
        repo.authenticate('klovett', 'klovett')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/')

        this_script = doc.agent('alg:klovett#projOne', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        #Add more resources on and change this.
        bbLocResource = doc.entity('bdp:15e7fa44-b9a8-42da-82e1-304e43460095', {'prov:label':'BB Loc Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        bbAlertResource = doc.entity('bdp:c8c54c49-3097-40fc-b3f2-c9508b8d393a', {'prov:label':'BB Alerts Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        treeLocResource = doc.entity('bod:ce863d38db284efe83555caf8a832e2a_1', {'prov:label':'Tree Loc Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        trashResource = doc.entity('bod:b09b9dd54c1241369080c0ee48895e85_10', {'prov:label':'Trash Resource', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        cambridgeResource = doc.entity('cdp:ufnx-m9uc', {'prov:label':'Cambridge Land', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        bostonResource = doc.entity('bdp:5b027436-5213-4be6-ab5f-485a03f74500', {'prov:label':'Boston Land', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_bbLocations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bbAlerts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_treeLocations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trashPickup = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_landCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_landBoston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_bbLocations, this_script)
        doc.wasAssociatedWith(get_bbAlerts, this_script)
        doc.wasAssociatedWith(get_treeLocations, this_script)
        doc.wasAssociatedWith(get_trashPickup, this_script)
        doc.wasAssociatedWith(get_landCambridge, this_script)
        doc.wasAssociatedWith(get_landBoston, this_script)
        

        doc.usage(get_bbLocations, bbLocResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=BB+Locations&$select=description'
                  }
                  )
        doc.usage(get_bbAlerts, bbAlertResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=BB+Alerts&$select=description, fullness'
                  }
                  )
        
        doc.usage(get_treeLocations, treeLocResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=TreeLocations&$select=features'
                  }
                  )
        
        doc.usage(get_trashPickup, trashResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Trash+Pickup&$select=features'
                  }
                  )
        
        doc.usage(get_landCambridge, cambridgeResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Land&$select=existing_units, land_use_category, land_use_description, location, location_1'
                  }
                  )

        doc.usage(get_landBoston, bostonResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Land&$select=Address'
                  }
                  )
        
        bbLocations = doc.entity('dat:klovett#bbLocations', {prov.model.PROV_LABEL:'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bbLocations, this_script)
        doc.wasGeneratedBy(bbLocations, get_bbLocations, endTime)
        doc.wasDerivedFrom(bbLocations, bbLocResource, get_bbLocations, get_bbLocations, get_bbLocations)

        bbAlerts = doc.entity('dat:klovett#bbAlerts', {prov.model.PROV_LABEL:'Big Belly Alerts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bbAlerts, this_script)
        doc.wasGeneratedBy(bbAlerts, get_bbAlerts, endTime)
        doc.wasDerivedFrom(bbAlerts, bbAlertResource, get_bbAlerts, get_bbAlerts, get_bbAlerts)
        
        treeLocations = doc.entity('dat:klovett#treeLocations', {prov.model.PROV_LABEL:'Tree Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(treeLocations, this_script)
        doc.wasGeneratedBy(treeLocations, get_treeLocations, endTime)
        doc.wasDerivedFrom(treeLocations, treeLocResource, get_treeLocations, get_treeLocations, get_treeLocations)

        trashPickup = doc.entity('dat:klovett#trashPickup', {prov.model.PROV_LABEL:'Trash Pickup Days', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trashPickup, this_script)
        doc.wasGeneratedBy(trashPickup, get_trashPickup, endTime)
        doc.wasDerivedFrom(trashPickup, trashResource, get_trashPickup, get_trashPickup, get_trashPickup)

        landCambridge = doc.entity('dat:klovett#landCambridge', {prov.model.PROV_LABEL:'Land Locations Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(landCambridge, this_script)
        doc.wasGeneratedBy(landCambridge, get_landCambridge, endTime)
        doc.wasDerivedFrom(landCambridge, cambridgeResource, get_landCambridge, get_landCambridge, get_landCambridge)
        
        landBoston = doc.entity('dat:klovett#landBoston', {prov.model.PROV_LABEL:'Land Locations Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(landBoston, this_script)
        doc.wasGeneratedBy(landBoston, get_landBoston, endTime)
        doc.wasDerivedFrom(landBoston, bostonResource, get_landBoston, get_landBoston, get_landBoston)

        repo.logout()
                  
        return doc

projOne.execute()
doc = projOne.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
