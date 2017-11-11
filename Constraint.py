import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import geojson
#from tqdm import tqdm
import pdb
import scipy.stats

class constraint(dml.Algorithm):
    contributor = 'francisz_jrashaan'
    
    reads = ['francisz_jrashaan.crime']
    
    writes = ['francisz_jrashaan.crimeData']
    
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')

        

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
        repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cam','https://data.cambridgema.gov/api/views/')
        
        
        this_script = doc.agent('alg:francisz_jrashaan#fzjr_retrievalalgorithm', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crime = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_streetlights = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5', {'prov:label':'Streetlights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_landuse = doc.entity('cam:srp4-fhjz/rows.json', {'prov:label':'Landuse', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_capopulation = doc.entity('cam:r4pm-qqje/rows', {'prov:label':'Cambridge Population Density', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_openspace = doc.entity('bdp:769c0a21-9e35-48de-a7b0-2b7dfdefd35e', {'prov:label':'Openspace', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_landuse = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_capopulation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_openspace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.wasAssociatedWith(get_landuse, this_script)
        doc.wasAssociatedWith(get_capopulation, this_script)
        doc.wasAssociatedWith(get_openspace, this_script)
        doc.usage(get_crime, resource_crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        doc.usage(get_streetlights, resource_streetlights, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval'
                            }
                            )
                  
        doc.usage(get_landuse, resource_landuse, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval'}
                            )
        doc.usage(get_capopulation, resource_capopulation, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval'}
                            )
        doc.usage(get_openspace, resource_openspace, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval'
                            }
                            )
        crime = doc.entity('dat:francisz_jrashaan#crime', {prov.model.PROV_LABEL:'crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_crime, get_crime, get_crime, get_crime)
                  
        streetlights = doc.entity('dat:francisz_jrashaan#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetlights, this_script)
        doc.wasGeneratedBy(streetlights, get_streetlights, endTime)
        doc.wasDerivedFrom(streetlights, resource_streetlights, get_streetlights, get_streetlights, get_streetlights)
                  
        landuse = doc.entity('dat:francisz_jrashaan#landuse', {prov.model.PROV_LABEL:'landuse', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(landuse, this_script)
        doc.wasGeneratedBy(landuse, get_landuse, endTime)
        doc.wasDerivedFrom(landuse, resource_landuse, get_landuse, get_landuse, get_landuse)
                  
        capopulation = doc.entity('dat:francisz_jrashaan#capopulation', {prov.model.PROV_LABEL:'capopulation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(capopulation, this_script)
        doc.wasGeneratedBy(capopulation, get_capopulation, endTime)
        doc.wasDerivedFrom(capopulation, resource_capopulation, get_capopulation, get_capopulation, get_capopulation)
                  
        openspace = doc.entity('dat:francisz_jrashaan#openspace', {prov.model.PROV_LABEL:'openspace', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(openspace, this_script)
        doc.wasGeneratedBy(openspace, get_openspace, endTime)
        doc.wasDerivedFrom(openspace, resource_openspace, get_openspace, get_openspace, get_openspace)
                  
        repo.logout()
                  
        return doc

constraint.execute()
doc = constraint.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof


