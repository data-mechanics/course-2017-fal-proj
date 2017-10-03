import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class districtCrimeAndTrashReports(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = ['adsouza_mcsmocha.ThreeReq',  'adsouza_mcsmocha.PWD', 'adsouza_mcsmocha.CrimeData']
    writes = ['adsouza_mcsmocha.districtCrimeAndTrashReports']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        repo.dropCollection('DistrictCrimeAndTrashReports')
        repo.createCollection('DistrictCrimeAndTrashReports')

        threeReq = repo['adsouza_mcsmocha.ThreeReq'].find()
        pubWorks = repo['adsouza_mcsmocha.PWD'].find_one()
        pubWorks = pubWorks["features"]
        pubWorks = [e["properties"] for e in pubWorks]
        crime = repo['adsouza_mcsmocha.CrimeData'].find_one()
        crime = crime["records"]
        crime = [e["fields"] for e in crime]

        # collect dictionary of districts and offense code groups
        crimeDistricts = []
        for e in crime:
            crimeDict = {}
            crimeDict['district'] = e['district']
            crimeDict['offense code group'] = e['offense_code_group']
            crimeDistricts.append(crimeDict)

        # collect dictionary of districts and type of request
        def keyInList(keyval, dictList):
            for e in dictList:
                if e["NAME"] in keyval or keyval in e["NAME"]:
                    return True
            return False

        selectPWDNeighborhoods = [e for e in threeReq if keyInList(e["neighborhood"], pubWorks)]

        def findKeyInList(keyval, dictList):
            for e in dictList:
                if e["NAME"] in keyval or keyval in e["NAME"]:
                    return e["DIST"]

        trashDistricts = []
        for e in threeReq:
            trashDict = {}
            if "Trash" in e["TYPE"]:
                dist = findKeyInList(e["neighborhood"], pubWorks)
                trashDict['district'] = dist
                trashDict['TYPE'] = e['TYPE']
            trashDistricts.append(trashDict)

        # aggregate common districts into one element, and unioning groups with a common
        # district

        # create unique list of districts
        districtList = []
        for e in crimeDistricts:
        	if e['district'] not in districtList:
        		districtList.append(e['district'])

       	# aggregate districts with crimes occurring in that district and trash reports made in that district
       	commonCrimes = []
       	for v in districtList:
       		commCrimeDict = {}
       		commCrimeDict['district'] = v
       		commCrimeDict['offense code group'] = []
       		commCrimeDict['TYPE'] = []
       		commonCrimes.append(commCrimeDict)

       	for e in commonCrimes:
       		for k in crimeDistricts:
       			if k['district'] == e['district'] and k['offense code group'] not in e['offense code group']:
       				e['offense code group'].append(k['offense code group'])
       		for k in trashDistricts:
       			if k['district'] == e['district'] and k['TYPE'] not in e['TYPE']:
       				e['TYPE'].append(k['TYPE'])

       	repo['adsouza_mcsmocha.districtCrimeAndTrashReports'].insert_many(commonCrimes)

       	repo.logout()

       	endTime = datetime.datetime.now()

       	return {"start": startTime, "end": endTime}

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
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # Additional resource
        doc.add_namespace('anb', 'https://data.boston.gov/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/')
        doc.add_namespace('ods', 'https://data.opendatasoft.com/')

        this_script = doc.agent('alg:adsouza_mcsmocha#districtCrimeAndTrashReports', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        three_resource = doc.entity('anb:2968e2c0-d479-49ba-a884-4ef523ada3c0', {'prov:label':'311 Requests, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        pwd_resource = doc.entity('bod:4b0f71af07664337975119c526f5a3a8_2', {'prov:label':'Public Works Districts, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        crime_resource = doc.entity('ods:crime-incident-reports-2017@boston', {'prov:label':'Crime Data, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_three = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_pwd = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_three, this_script)
        doc.wasAssociatedWith(get_pwd, this_script)
        doc.wasAssociatedWith(get_crime, this_script)

        doc.usage(get_three, three_resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?type=311+Requests&$CASE_TITLE,TYPE,QUEUE,Department,Location,pwd_district,neighborhood,neighborhood_services_district,LOCATION_STREET_NAME,LOCATION_ZIPCODE'
            }
            )
        doc.usage(get_pwd, pwd_resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?type=Public+Works+Districts&$PWD,NAME,COMBO,DIST,OBJECTID'
            }
            )
        doc.usage(get_crime, crime_resource, startTime, None,
        	{prov.model.PROV_TYPE:'ont:Retrieval',
        	'ont:Query':'?type=Crime+Data&$occured_on_date,offense_description,street,ahooting,offense_code_group,district,reporting_area,location'
        	}
        	)

        req_311 = doc.entity('dat:adsouza_mcsmocha#ThreeReq', {prov.model.PROV_LABEL:'311 Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(req_311, this_script)
        doc.wasGeneratedBy(req_311, get_three, endTime)
        doc.wasDerivedFrom(req_311, three_resource, get_three, get_three, get_three)

        pwd = doc.entity('dat:adsouza_mcsmocha#PWD', {prov.model.PROV_LABEL:'Public Works Districts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pwd, this_script)
        doc.wasGeneratedBy(pwd, get_pwd, endTime)
        doc.wasDerivedFrom(pwd, pwd_resource, get_pwd, get_pwd, get_pwd)

        crime = doc.entity('dat:adsouza_mcsmocha#CrimeData', {prov.model.PROV_LABEL:'Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, crime_resource, get_crime, get_crime, get_crime)

        repo.logout()

        return doc

districtCrimeAndTrashReports.execute()
doc = districtCrimeAndTrashReports.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
