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

class transformation_restaurants_within_1mile(dml.Algorithm):
    contributor = 'wenjun'
    reads =[]
    writes = ['wenjun.Property_Assessment','wenjun.foodCambridge','wenjun.foodBoston',
              'wenjun.parkingMetersBoston','wenjun.parkingMetersCambridge',
              'wenjun.crimeBoston','wenjun.k_means_coordinates_safty','wenjun.k_means_coordinates_restaurants']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wenjun','wenjun')

        yelpRestaurants = repo['wenjun.yelpRestaurants']
        k_means_ParkingMeters = repo['wenjun.k_means_parkingMeters']

        repo.dropCollection("k_means_coordinates_restaurants")
        repo.createCollection("k_means_coordinates_restaurants")

        cleanyelpRestaurants =[]

        #print (crimeBoston)
        
        for entry in yelpRestaurants.find():
            #print(entry.keys())
            cleanyelpRestaurants.append([entry['coordinates']['longitude'],entry['coordinates']['latitude']])
            
        for entry in k_means_ParkingMeters.find():
            #print(entry.keys())
            
            lon1 = entry['coordinates'][0]
            lat1 = entry['coordinates'][1]
            restaurants_num =0
            for x in cleanyelpRestaurants:
                if x[0]!=None and x[1]!= None:
                    lon2 = x[0]
                    lat2 = x[1]
                    #print(x)
                    d = distance(lon1,lat1,lon2,lat2)

                    if d<=1:
                        restaurants_num = restaurants_num+1

            insert = {'coordinates':entry['coordinates'],'restaurant_number':restaurants_num}
            repo['wenjun.k_means_coordinates_restaurants'].insert_one(insert)

        #for entry in repo['wenjun.k_means_coordinates_restaurants'].find():
            #print(entry)    
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
        doc.add_namespace('ydp','https://api.yelp.com/v3/businesses/search')
        this_script = doc.agent('alg:wenjun#transformation_restaurants_within_1mile',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_yelpRestaurants = doc.entity('dat:wenjun#yelpRestaurants',
                                               {'prov:label': 'restaurants in City of Boston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

        resource_k_means_ParkingMeters = doc.entity('dat:wenjun#k_means_ParkingMeters',
                                               {'prov:label': 'k_means_ParkingMeters',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

       # Activities' Associations with Agent
        transform_restaurants_within_1mile= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "transform restaurants within 1 mile",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(transform_restaurants_within_1mile, this_script)

        # Record which activity used which resource
        doc.usage(transform_restaurants_within_1mile, resource_yelpRestaurants, startTime)
        doc.usage(transform_restaurants_within_1mile, resource_k_means_ParkingMeters, startTime)

        # Result dataset entity
        k_means_ParkingMeters_restaurants  = doc.entity('dat:wenjun#k_means_coordinates_restaurants',
                                       {prov.model.PROV_LABEL: 'k means Parking Meters Restaurants',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(k_means_ParkingMeters_restaurants, this_script)
        doc.wasGeneratedBy(k_means_ParkingMeters_restaurants, transform_restaurants_within_1mile, endTime)
        doc.wasDerivedFrom(k_means_ParkingMeters_restaurants, resource_yelpRestaurants, k_means_ParkingMeters_restaurants, k_means_ParkingMeters_restaurants,
                           k_means_ParkingMeters_restaurants)
        doc.wasDerivedFrom(k_means_ParkingMeters_restaurants, resource_k_means_ParkingMeters, k_means_ParkingMeters_restaurants, k_means_ParkingMeters_restaurants,
                           k_means_ParkingMeters_restaurants)


        repo.logout()
        return doc        

'''
transformation_restaurants_within_1mile.execute()
doc = transformation_restaurants_within_1mile.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
