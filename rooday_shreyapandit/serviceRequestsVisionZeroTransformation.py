import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import rtree

class serviceRequestsVisionZeroTransformation(dml.Algorithm):
    def union(R, S):
        return R + S
    def intersect(R, S):
        return [t for t in R if t in S]
    def product(R, S):
        return [(t,u) for t in R for u in S]
    def select(R, s):
        return [t for t in R if s(t)]
    def aggregate(R,f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    def removeDuplicates(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x)) and x != " "]

    def betweenDates(t):
        startDate = datetime.datetime(2016, 1, 1).date()
        endDate = datetime.datetime(2017, 1, 1).date()
        date = datetime.datetime.strptime(t['open_dt'], '%Y-%m-%dT%H:%M:%S').date()
        return (startDate <= date <= endDate)

    def cleanServices(t):
        return (str(datetime.datetime.strptime(t['open_dt'], '%Y-%m-%dT%H:%M:%S').date()), t['TYPE'], t['LOCATION_ZIPCODE'], [t['Longitude'], t['Latitude']])

    def cleanConcerns(t):
        time = t["properties"]["REQUESTDATE"].split(".")[0]
        return (str(datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S').date()), t["properties"]["REQUESTTYPE"], t['geometry']['coordinates'])

    contributor = 'rooday_shreyapandit'
    reads = ['rooday_shreyapandit.visionzero',
              'rooday_shreyapandit.servicerequests']
    writes = ['rooday_shreyapandit.serviceRequestsWithVisionZero']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        serviceRequestsData = repo['rooday_shreyapandit.servicerequests']
        visionZeroData = repo['rooday_shreyapandit.visionzero']

        print("Filtering Service Requests for 2016 only...")
        serviceRequests2016 = serviceRequestsVisionZeroTransformation.select(serviceRequestsData.find(), serviceRequestsVisionZeroTransformation.betweenDates)
        print("Projecting Service Requests for relevant info only...")
        cleanedServices = serviceRequestsVisionZeroTransformation.project(serviceRequests2016, serviceRequestsVisionZeroTransformation.cleanServices)
        print("Projecting Concerns for relevant info only...")
        cleanedConcerns = serviceRequestsVisionZeroTransformation.project(visionZeroData.find(), serviceRequestsVisionZeroTransformation.cleanConcerns)

        print("Building RTree...")
        serviceRequestsRtree = rtree.index.Index()
        for i in range(len(cleanedServices)):
            coords = cleanedServices[i][3]
            bounds = (float(coords[0]), float(coords[1]), float(coords[0]), float(coords[1]))
            serviceRequestsRtree.insert(i, bounds)

        print("Finding nearest requests...")
        finalList = []
        for entry in cleanedConcerns:
            coords = entry[2]
            bounds = (float(coords[0]), float(coords[1]), float(coords[0]), float(coords[1]))
            nearestIndices = serviceRequestsRtree.nearest(bounds, 3)
            nearest = [cleanedServices[i] for i in nearestIndices]
            finalList.append({'date': entry[0], 'type': entry[1], 'coords': entry[2], 'nearestRequests': nearest})

        print("Sorting final list...")
        finalList.sort(key=lambda x: x['date'])

        print(finalList[0])

    
        print('DONE!')
        repo.dropCollection('serviceRequestsWithVisionZero')
        repo.createCollection('serviceRequestsWithVisionZero')
        repo['rooday_shreyapandit.serviceRequestsWithVisionZero'].insert_many(finalList)

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:rooday_shreyapandit#serviceRequestsVisionZeroTransformation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_311 = doc.entity('dat:rooday_shreyapandit#servicerequests', {'prov:label': 'Service Request Data for Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_visionzero = doc.entity('dat:rooday_shreyapandit#visionzero', {'prov:label': 'Safety Concerns Data for Boston', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_serviceRequestsVisionZero = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_serviceRequestsVisionZero, this_script)

        doc.usage(get_serviceRequestsVisionZero, resource_311, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_serviceRequestsVisionZero, resource_visionzero, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        crime_weather = doc.entity('dat:rooday_shreyapandit#crimesByDateAndWeather', {prov.model.PROV_LABEL: 'Number of Crimes and Weather Type by Date', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crime_weather, this_script)
        doc.wasGeneratedBy(crime_weather, get_serviceRequestsVisionZero, endTime)
        doc.wasDerivedFrom(crime_weather, resource_311, get_serviceRequestsVisionZero, get_serviceRequestsVisionZero, get_serviceRequestsVisionZero)
        doc.wasDerivedFrom(crime_weather, resource_visionzero, get_serviceRequestsVisionZero, get_serviceRequestsVisionZero, get_serviceRequestsVisionZero)
        repo.logout()

        return doc

serviceRequestsVisionZeroTransformation.execute()
doc = serviceRequestsVisionZeroTransformation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))