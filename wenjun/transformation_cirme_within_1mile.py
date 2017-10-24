import urllib.request
#import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from math import sin, cos, sqrt, atan2, radians


def distance(lon1,lat1,lon2,lat2):
    R = 6371.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    
    lat2 = radians(float(lat2))
    lon2 = radians(float(lon2))

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    distance = distance * 1.621371

    return distance

class transformation_cirme_within_1mile(dml.Algorithm):
    contributor = 'wenjun'
    reads =[]
    writes = ['wenjun.Property_Assessment','wenjun.foodCambridge','wenjun.foodBoston',
              'wenjun.parkingMetersBoston','wenjun.parkingMetersCambridge','wenjun.crimeBoston','wenjun.k_means_coordinates_safty']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wenjun','wenjun')

        crimeBoston = repo['wenjun.crimeBoston']
        k_means_ParkingMeters = repo['wenjun.k_means_parkingMeters']

        repo.dropCollection("k_means_coordinates_safty")
        repo.createCollection("k_means_coordinates_safty")

        cleanCrimeBoston =[]

        #print (crimeBoston)
        
        for entry in crimeBoston.find():
            #print(entry.keys())
            cleanCrimeBoston.append([entry['Long'],entry['Lat']])

        for entry in k_means_ParkingMeters.find():
            #print(entry.keys())
            
            lon1 = entry['coordinates'][0]
            lat1 = entry['coordinates'][1]
            crime_num =0
            for x in cleanCrimeBoston:
                if x[0]!=None and x[1]!= None:
                    lon2 = x[0]
                    lat2 = x[1]
                    #print(x)
                    d = distance(lon1,lat1,lon2,lat2)

                    if d<=1:
                        crime_num = crime_num+1

            insert = {'coordinates':entry['coordinates'],'crime_number':crime_num}
            repo['wenjun.k_means_coordinates_safty'].insert_one(insert)

     
        repo.logout()
        

        endTime = datetime.datetime.now()

        

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wenjun', 'wenjun')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset')
        
        #doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        this_script = doc.agent('alg:wenjun#transformation_cirme_within_1mile',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_crimeBoston = doc.entity('dat:wenjun#crimeBoston',
                                               {'prov:label': 'crime in City of Boston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

        resource_k_means_ParkingMeters = doc.entity('dat:wenjun#k_means_ParkingMeters',
                                               {'prov:label': 'k_means_ParkingMeters',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

       # Activities' Associations with Agent
        transform_cirme_within_1mile= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "transform cirme within 1 mile",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(transform_cirme_within_1mile, this_script)

        # Record which activity used which resource
        doc.usage(transform_cirme_within_1mile, resource_crimeBoston, startTime)
        doc.usage(transform_cirme_within_1mile, resource_k_means_ParkingMeters, startTime)

        # Result dataset entity
        k_means_ParkingMeters_safety  = doc.entity('dat:wenjun#k_means_ParkingMeters_safety',
                                       {prov.model.PROV_LABEL: 'k means ParkingMeters Safety',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(k_means_ParkingMeters_safety, this_script)
        doc.wasGeneratedBy(k_means_ParkingMeters_safety, transform_cirme_within_1mile, endTime)
        doc.wasDerivedFrom(k_means_ParkingMeters_safety, resource_crimeBoston, transform_cirme_within_1mile, transform_cirme_within_1mile,
                           transform_cirme_within_1mile)
        doc.wasDerivedFrom(k_means_ParkingMeters_safety, resource_k_means_ParkingMeters, transform_cirme_within_1mile, transform_cirme_within_1mile,
                           transform_cirme_within_1mile)


        repo.logout()
        return doc        

'''
transformation_cirme_within_1mile.execute()
doc = transformation_cirme_within_1mile.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
