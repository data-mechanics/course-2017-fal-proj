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
    
    def takeFirst(elem):
        return elem[0]
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('francisz_jrashaan','francisz_jrashaan')

        data = [('North End', [0, 3, 236, 240]), ('Bay Village', [0, 0, 24, 42]), ('East Boston', [0, 19, 222, 3544]), ('Leather District', [8, 8, 34, 43]), ('Allston', [0, 1, 1888, 1994]), ('Hyde Park', [0, 0, 569, 1163]), ('Roslindale', [0, 0, 450, 608]), ('Charlestown', [0, 7, 189, 455]), ('Back Bay', [4, 17, 432, 817]), ('South End', [0, 0, 116, 150]), ('Downtown', [4, 33, 160, 420]), ('Dorchester', [0, 7, 1382, 3710]), ('South Boston Waterfront', [15, 7, 102, 222]), ('West Roxbury', [0, 0, 559, 708]), ('Longwood Medical Area', [0, 11, 136, 154]), ('Mission Hill', [0, 11, 135, 161]), ('Roxbury', [0, 7, 315, 525]), ('Beacon Hill', [1, 16, 149, 391]), ('Mattapan', [0, 0, 348, 627]), ('Harbor Islands', [0, 0, 0, 155]), ('Brighton', [0, 0, 983, 1466]), ('South Boston', [0, 1, 410, 1061]), ('West End', [0, 5, 387, 549]), ('Fenway', [4, 21, 893, 1034]), ('Chinatown', [11, 21, 74, 112]), ('Jamaica Plain', [0, 0, 356, 919])]
        scores = []
        newdata = []
        for i in data:
            temp = i[1]
            score += temp[0] * 7 + temp[1] * 3 + temp[2] * 5
            scores += [(score, i[0], i[1])]
        
        
        scores.sort(key=takeFirst)
        average = 0
        count = 0
        for i in scores:
            average += i[0]
            count += 1
        average = average / count
        budget = 1000000
        Cstation = 7000
        Hstation = 3000
        Bnetwork = 10000
        flag = True
        while(flag):
            lowest = scores[0]

    


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


