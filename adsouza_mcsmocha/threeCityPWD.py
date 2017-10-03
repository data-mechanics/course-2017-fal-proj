import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class request311Req(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = ['adsouza_mcsmocha.ThreeReq', 'adsouza_mcsmocha.CityScore', 'adsouza_mcsmocha.PWD']
    writes = ['adsouza_mcsmocha.threeCityPWD']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')

        repo.dropCollection('CityScoreofPWDAreas')
        repo.createCollection('CityScoreofPWDAreas')

        threeReq = repo['adsouza_mcsmocha.ThreeReq'].find()
        cityScore = repo['adsouza_mcsmocha.CityScore'].find()
        pubWorks = repo['adsouza_mcsmocha.PWD'].find()

        # Select entries in CityScore that pertain to Trash using select
        selectCityScorePWD = [e for e in cityScore if "TRASH" in e["CTY_SCR_NAME"]]

        # Union all the Quarterly Trash scores within the above list
        unionTrashScore = [e["CTY_SCR_NBR_QT_01"] for e in selectCityScorePWD]

        # Aggregate the average of the collective Trash score
        aggAverage = sum(unionTrashScore) / len(unionTrashScore)

        # Select entries in 311 Requests that pertain to Public Works Department
        select311Trash = [e for e in threeReq if "Trash" in e["TYPE"]]

        def keyInList(keyval, dictList):
            for e in dictList:
                if e["NAME"] in keyval or keyval in e["NAME"]:
                    return True
            return False

        # Select 311 Trash entries based on existing neighborhoods in the PWD 
        # dataset
        selectPWDNeighborhoods = [e for e in select311Trash if keyInList(e["neighborhood"], pubWorks)]

        def findKeyInList(keyval, dictList):
            for e in dictList:
                if e["NAME"] in keyval or keyval in e["NAME"]:
                    return e["DIST"]

        # Intersect neighborhoods from the PWD dataset and the 311 Trash Requests
        # to obtain the district numbers of the requests, then make the average score
        # another column
        intersect311Neighborhood = []
        for e in selectPWDNeighborhoods:
            dist = findKeyInList(e["neighborhood"], pubWorks)
            e["district"] = dist
            e["average city score"] = aggAverage
            intersect311Neighborhood.append(e)

        repo['adsouza_mcsmocha.ThreeCityPWD'].insert_many(intersect311Neighborhood)

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

        this_script = doc.agent('alg:adsouza_mcsmocha#threeCityPWD', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        city_resource = doc.entity('anb:5bce8e71-5192-48c0-ab13-8faff8fef4d7', {'prov:label':'CityScore, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        three_resource = doc.entity('anb:2968e2c0-d479-49ba-a884-4ef523ada3c0', {'prov:label':'311 Requests, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        pwd_resource = doc.entity('bod:4b0f71af07664337975119c526f5a3a8_2', {'prov:label':'Public Works Districts, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_city = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_three = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_pwd = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_city, this_script)
        doc.wasAssociatedWith(get_three, this_script)
        doc.wasAssociatedWith(get_pwd, this_script)

        doc.usage(get_city, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?type=CityScore&$CTY_SCR_NAME,CTY_SCR_NBR_QT_01,CTY_SCR_TGT_01'
            }
            )
        doc.usage(get_three, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?type=311+Requests&$CASE_TITLE,TYPE,QUEUE,Department,Location,pwd_district,neighborhood,neighborhood_services_district,LOCATION_STREET_NAME,LOCATION_ZIPCODE'
            }
            )
        doc.usage(get_pwd, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
            'ont:Query':'?type=Public+Works+Districts&$PWD,NAME,COMBO,DIST,OBJECTID'
            }
            )

        city_score = doc.entity('dat:adsouza_mcsmocha#CityScore', {prov.model.PROV_LABEL:'CityScore', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(city_score, this_script)
        doc.wasGeneratedBy(city_score, get_city, endTime)
        doc.wasDerivedFrom(city_score, resource, get_city, get_city, get_city)

        req_311 = doc.entity('dat:adsouza_mcsmocha#ThreeReq', {prov.model.PROV_LABEL:'311 Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(req_311, this_script)
        doc.wasGeneratedBy(req_311, get_three, endTime)
        doc.wasDerivedFrom(req_311, resource, get_three, get_three, get_three)

        pwd = doc.entity('dat:adsouza_mcsmocha#PWD', {prov.model.PROV_LABEL:'Public Works Districts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pwd, this_script)
        doc.wasGeneratedBy(pwd, get_pwd, endTime)
        doc.wasDerivedFrom(pwd, resource, get_pwd, get_pwd, get_pwd)

        repo.logout()

        return doc

threeCityPWD.execute()
doc = threeCityPWD.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))