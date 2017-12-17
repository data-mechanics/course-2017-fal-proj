from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl


class getBostonZoning(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.BostonZoning']
    ssl._create_default_https_context = ssl._create_unverified_context

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/eebd3daed05a45678894db30d9bf0cfb_0.geojson'
        response = urlopen(url).read().decode("utf-8")

        r = json.loads(response)

        dontInclude=['Harborpark: North End Waterfront', 'Harborpark: Charlestown Waterfront', 'Harborpark: Dorchester Bay/Neponset River Waterfront', 'Harborpark: Fort Point Waterfront','Boston Harbor']
        goodData = {}
        for i in range(len(r['features']) - 1):
            if r['features'][i]['properties']['DISTRICT'] in dontInclude:
                continue
            else:
                goodData[r['features'][i]['properties']['DISTRICT']] = r['features'][i]['geometry']

        repo.dropCollection("BostonZoning")
        repo.createCollection("BostonZoning")
        repo['biel_otis.BostonZoning'].insert_many([goodData])
        repo['biel_otis.BostonZoning'].metadata({'complete':True})
        print(repo['biel_otis.BostonZoning'].metadata())
        


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
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('zones', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:biel_otis#getBostonZoning', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('zones:eebd3daed05a45678894db30d9bf0cfb_0', {'prov:label':'Multipolygon Shape of Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        output_resource = doc.entity('dat:biel_otis#BostonZoning', {prov.model.PROV_LABEL: 'Dataset containing geojson for shapefiles of Boston Neighborhoods.', prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    
        
        #Associations
        doc.wasAssociatedWith(this_run, this_script)
     
        #Usages
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        #Generated
        doc.wasGeneratedBy(output_resource, this_run, endTime)


        #Attributions
        doc.wasAttributedTo(output_resource, this_script)

        #Derivations
        doc.wasDerivedFrom(output_resource, resource, this_run, this_run, this_run)
        repo.logout()
          
        return doc


## eof
