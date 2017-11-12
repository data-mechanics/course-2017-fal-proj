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
        resource_chargeStations = doc.entity('bdp:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'chargeStations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_Hubways = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5', {'prov:label':'Hubways', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_bikeNetwork = doc.entity('cam:srp4-fhjz/rows.json', {'prov:label':'bikeNetworks', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_capopulation = doc.entity('cam:r4pm-qqje/rows', {'prov:label':'Cambridge Population Density', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_openspace = doc.entity('bdp:769c0a21-9e35-48de-a7b0-2b7dfdefd35e', {'prov:label':'Openspace', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_chargeStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Hubways = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bikeNetworks = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_capopulation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_openspace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_chargeStations, this_script)
        doc.wasAssociatedWith(get_Hubways, this_script)
        doc.wasAssociatedWith(get_bikeNetworks, this_script)
        doc.wasAssociatedWith(get_capopulation, this_script)
        doc.wasAssociatedWith(get_openspace, this_script)
        doc.usage(get_chargeStations, resource_chargeStations, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_Hubways, resource_Hubways, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_bikeNetworks, resource_bikeNetwork, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_capopulation, resource_capopulation, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_openspace, resource_openspace, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        chargeStations = doc.entity('dat:francisz_jrashaan#chargeStations', {prov.model.PROV_LABEL:'chargeStations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(chargeStations, this_script)
        doc.wasGeneratedBy(crime, get_chargeStations, endTime)
        doc.wasDerivedFrom(crime, resource_chargeStations, get_chargeStations, get_chargeStations, get_chargeStations)
                  
        Hubways = doc.entity('dat:francisz_jrashaan#Hubways', {prov.model.PROV_LABEL:'Hubways', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Hubways, this_script)
        doc.wasGeneratedBy(Hubways, get_streetlights, endTime)
        doc.wasDerivedFrom(Hubways, resource_Hubways, get_Hubways, get_Hubways, get_Hubways)
                  
        bikeNetwork = doc.entity('dat:francisz_jrashaan#bikeNetwork', {prov.model.PROV_LABEL:'bikeNetwork', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeNetwork, this_script)
        doc.wasGeneratedBy(bikeNetwork, get_bikeNetworks, endTime)
        doc.wasDerivedFrom(bikeNetwork, resource_bikeNetwork, get_bikeNetworks, get_bikeNetworks, get_bikeNetworks)
                  
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


