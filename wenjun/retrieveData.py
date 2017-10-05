from __future__ import print_function
import urllib.request
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode
#import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

import pprint
import requests
import sys
import argparse

def search(bearer_token, term, location,API_HOST,SEARCH_PATH):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        #'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, bearer_token, url_params=url_params)

def obtain_bearer_token(host, path,CLIENT_ID,CLIENT_SECRET,GRANT_TYPE):
    """Given a bearer token, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        str: OAuth bearer token, obtained using client_id and client_secret.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    assert CLIENT_ID, "Please supply your client_id."
    assert CLIENT_SECRET, "Please supply your client_secret."
    data = urlencode({
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': GRANT_TYPE,
    })
    headers = {
        'content-type': 'application/x-www-form-urlencoded',
    }
    response = requests.request('POST', url, data=data, headers=headers)
    bearer_token = response.json()['access_token']
    return bearer_token

def request(host, path, bearer_token, url_params=None):
    """Given a bearer token, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        bearer_token (str): OAuth bearer token, obtained using client_id and client_secret.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % bearer_token,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


class retrieveData(dml.Algorithm):
    '''
    try:
        # For Python 3.0 and later
        from urllib.error import HTTPError
        from urllib.parse import quote
        from urllib.parse import urlencode
    except ImportError:
        # Fall back to Python 2's urllib2 and urllib
        from urllib2 import HTTPError
        from urllib import quote
        from urllib import urlencode
    '''

    contributor = 'wenjun'
    reads =[]
    writes = ['wenjun.Property_Assessment','wenjun.foodCambridge','wenjun.foodBoston',
              'wenjun.parkingMetersBoston','wenjun.parkingMetersCambridge','wenjun.crimeBoston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wenjun','wenjun')

        url = 'https://data.boston.gov/api/action/datastore_search?resource_id=062fc6fa-b5ff-4270-86cf-202225e40858&limit=5'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r= json.loads(response)
        
        #r = json.loads(response)
        #r = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Property_Assessment")
        repo.createCollection("Property_Assessment")
        #for key, value in r.items() :
        #    print (key)
        #print(type(r['result']['records']))
        #print((r['result']['records']))
        dictlist=[]
        #for key, value in r['result']['records'].items():
         #   temp = [key,value]
         #   dictlist.append(temp)
        #print(type(dictlist))
        #print(dictlist)
        repo['wenjun.Property_Assessment'].insert_many(r['result']['records'])
        repo['wenjun.Property_Assessment'].metadata({'complete':True})
        print(repo['wenjun.Property_Assessment'].metadata())

#----------------------------------------------------------------------------------
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r= json.loads(response)
        #print(r)
        #r = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crimeBoston")
        repo.createCollection("crimeBoston")
        dictlist=[]
        repo['wenjun.crimeBoston'].insert_many(r['result']['records'])
        repo['wenjun.crimeBoston'].metadata({'complete':True})
        print(repo['wenjun.Property_Assessment'].metadata())

#----------------------------------------------------------------------
        url='https://data.boston.gov/api/3/action/datastore_search?resource_id=f1e13724-284d-478c-b8bc-ef042aa5b70b'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r= json.loads(response)
        #r = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("foodBoston")
        repo.createCollection("foodBoston")
        #print(r)
        repo['wenjun.foodBoston'].insert_many(r['result']['records'])
        repo['wenjun.foodBoston'].metadata({'complete':True})
        print(repo['wenjun.foodBoston'].metadata())

#------------------------------------------------------------------       
        #client = dml.pymongo.MongoClient()
        #repo = client.repo
        #repo.authenticate('wenjun','wenjun')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r= json.loads(response)
        #r = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("parkingMetersBoston")
        repo.createCollection("parkingMetersBoston")
        #print(r)
        repo['wenjun.parkingMetersBoston'].insert_many(r['features'])
        repo['wenjun.parkingMetersBoston'].metadata({'complete':True})
        print(repo['wenjun.parkingMetersBoston'].metadata())

        url = 'https://api.yelp.com/v2/search?term=cream+puffs&location=Boston'
        

#--------------------------------------------------------------
        
        #cambridge parking
        #https://data.cambridgema.gov/resource/up94-ihbw.json
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("up94-ihbw")
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        repo.dropCollection("parkingMetersCambridge")
        repo.createCollection("parkingMetersCambridge")
        repo['wenjun.parkingMetersCambridge'].insert_many(r)
        repo['wenjun.parkingMetersCambridge'].metadata({'complete': True})
        print(repo['wenjun.parkingMetersCambridge'].metadata())

#--------------------------------------------------------------
        #url=https://data.cambridgema.gov/resource/38qx-ym6k.json

        
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("38qx-ym6k")
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        repo.dropCollection("foodCambridge")
        repo.createCollection("foodCambridge")
        repo['wenjun.foodCambridge'].insert_many(r)
        repo['wenjun.foodCambridge'].metadata({'complete': True})
        print(repo['wenjun.foodCambridge'].metadata())


        url = 'http://datamechanics.io/data/wenjun/censusincomedata.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("incomeBoston")
        repo.createCollection("incomeBoston")
        repo['wenjun.incomeBoston'].insert_many(r)
        repo['wenjun.incomeBoston'].metadata({'complete':True})
        print(repo['wenjun.neighborhoodsBoston'].metadata())

        url='http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r= json.loads(response)
        #r = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("neighborhoodsBoston")
        repo.createCollection("neighborhoodsBoston")
        #print(r['type'])
        repo['wenjun.neighborhoodsBoston'].insert_many(r['features'])
        repo['wenjun.neighborhoodsBoston'].metadata({'complete':True})
        print(repo['wenjun.neighborhoodsBoston'].metadata())

#----------------------------
            # API constants, you shouldn't have to change these.





        with open('auth.json') as cred_file:
            data = json.load(cred_file)
            CLIENT_ID = data['yelp']['CLIENT_ID']
            CLIENT_SECRET = data['yelp']['CLIENT_SECRET']
            
            API_HOST = 'https://api.yelp.com'
            SEARCH_PATH = '/v3/businesses/search'
            BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.
            TOKEN_PATH = '/oauth2/token'
            GRANT_TYPE = 'client_credentials'


            # Defaults for our simple example.
            DEFAULT_TERM = 'restaurant'
            DEFAULT_LOCATION = 'Boston, MA'
            SEARCH_LIMIT = 3
            
            parser =argparse.ArgumentParser()
            #print("2")
            parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                                type=str, help='Search term (default: %(default)s)')
            #print("3")
            parser.add_argument('-l', '--location', dest='location',
                                default=DEFAULT_LOCATION, type=str,

                                help='Search location (default: %(default)s)')
            #print("4")
            #print(parser)
            args, unknown = parser.parse_known_args()
            input_values = args
            #print("5")
            try:
                
                bearer_token = obtain_bearer_token(API_HOST, TOKEN_PATH,CLIENT_ID,CLIENT_SECRET,GRANT_TYPE)
                #print("6")
                
                r = search(bearer_token,input_values.term, input_values.location,API_HOST,SEARCH_PATH)
                r = json.loads(json.dumps(r, sort_keys=True, indent=2))
                #print(r)
                repo.dropCollection("yelpRestaurants")
                repo.createCollection("yelpRestaurants")
                repo['wenjun.yelpRestaurants'].insert_many(r["businesses"])
                repo['wenjun.yelpRestaurants'].metadata({'complete': True})
                print(repo['wenjun.yelpResturants'].metadata())
            except HTTPError as error:
                sys.exit(
                    'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                        error.code,
                        error.url,
                        error.read(),
                    )
                )
#----------------------------
                
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
        
        #doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        this_script = doc.agent('alg:wenjun#retrieveData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_yelpRestaurants = doc.entity('dat:wenjun#yelpRestaurants',
                                               {'prov:label': 'restaurants in City of Boston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

        resource_crimeBoston = doc.entity('dat:wenjun#crimeBoston',
                                               {'prov:label': 'crimeBoston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})
        
        resource_parkingMetersBoston = doc.entity('dat:wenjun#parkingMetersBoston',
                                               {'prov:label': 'crimeBoston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})
        resource_neighborhoodsBoston = doc.entity('dat:wenjun#neighborhoodsBoston',
                                               {'prov:label': 'neighborhoodsBoston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})
        resource_incomeBoston = doc.entity('dat:wenjun#incomeBoston',
                                               {'prov:label': 'incomeBoston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

       # Activities' Associations with Agent
        get_resource_yelpRestaurants= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "get_resource_yelpRestaurants",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})
        get_resource_crimeBoston= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "get_resource_crimeBoston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})
        get_resource_parkingMetersBoston= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "get_resource_parkingMetersBoston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})
        get_resource_neighborhoodsBoston= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "get_resource_neighborhoodsBoston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})
        get_resource_incomeBoston= doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: " get_resource_incomeBoston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(get_resource_yelpRestaurants, this_script)
        doc.wasAssociatedWith(get_resource_crimeBoston, this_script)
        doc.wasAssociatedWith(get_resource_parkingMetersBoston, this_script)
        doc.wasAssociatedWith(get_resource_neighborhoodsBoston, this_script)
        doc.wasAssociatedWith(get_resource_incomeBoston, this_script)

        # Record which activity used which resource
        doc.usage(get_resource_yelpRestaurants, resource_yelpRestaurants, startTime)
        doc.usage(get_resource_crimeBoston, resource_crimeBoston, startTime)
        doc.usage(get_resource_parkingMetersBoston, resource_parkingMetersBoston, startTime)
        doc.usage(get_resource_neighborhoodsBoston, resource_neighborhoodsBoston, startTime)
        doc.usage(get_resource_incomeBoston,resource_incomeBoston, startTime)

        # Result dataset entity
        yelpRestaurants  = doc.entity('dat:wenjun#yelpRestaurants',
                                       {prov.model.PROV_LABEL: 'yelpRestaurants',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        crimeBoston  = doc.entity('dat:wenjun#crimeBoston',
                                       {prov.model.PROV_LABEL: 'crimeBoston',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        parkingMetersBoston  = doc.entity('dat:wenjun#parkingMetersBoston',
                                       {prov.model.PROV_LABEL: 'parkingMetersBoston',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        neighborhoodsBoston  = doc.entity('dat:wenjun#neighborhoodsBoston',
                                       {prov.model.PROV_LABEL: 'neighborhoodsBoston',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        incomeBoston  = doc.entity('dat:wenjun#incomeBoston',
                                       {prov.model.PROV_LABEL: 'incomeBoston',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(yelpRestaurants, this_script)
        doc.wasAttributedTo(crimeBoston, this_script)
        doc.wasAttributedTo(parkingMetersBoston, this_script)
        doc.wasAttributedTo(neighborhoodsBoston, this_script)
        doc.wasAttributedTo(incomeBoston, this_script)
        
        doc.wasGeneratedBy(yelpRestaurants, get_resource_yelpRestaurants, endTime)
        doc.wasGeneratedBy(crimeBoston, get_resource_crimeBoston, endTime)
        doc.wasGeneratedBy(parkingMetersBoston, get_resource_parkingMetersBoston, endTime)
        doc.wasGeneratedBy(neighborhoodsBoston, get_resource_neighborhoodsBoston, endTime)
        doc.wasGeneratedBy(incomeBoston, get_resource_incomeBoston, endTime)

        
        doc.wasDerivedFrom(yelpRestaurants, resource_yelpRestaurants, get_resource_yelpRestaurants, get_resource_yelpRestaurants,
                           get_resource_yelpRestaurants)
        doc.wasDerivedFrom(crimeBoston, resource_crimeBoston, get_resource_crimeBoston, get_resource_crimeBoston,
                           get_resource_crimeBoston)
        doc.wasDerivedFrom(parkingMetersBoston, resource_parkingMetersBoston, get_resource_parkingMetersBoston, get_resource_parkingMetersBoston,
                           get_resource_parkingMetersBoston)
        doc.wasDerivedFrom(neighborhoodsBoston, resource_neighborhoodsBoston, get_resource_neighborhoodsBoston, get_resource_neighborhoodsBoston,
                           get_resource_neighborhoodsBoston)
        doc.wasDerivedFrom(incomeBoston, resource_incomeBoston, get_resource_incomeBoston, get_resource_incomeBoston,
                           get_resource_incomeBoston)


        repo.logout()
        return doc        
        
'''
retrieveData.execute()
doc = retrieveData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
