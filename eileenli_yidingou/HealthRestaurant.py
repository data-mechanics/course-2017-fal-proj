import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict


def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]

def cro(x,y):
    all_cord = []
    for key,value in x.items():
        for key2, value2 in y.items():
            all_cord.append([key,(key2,distance(value,value2))])
    return all_cord


def comb(ls):
    result ={}
    for k,v in ls:
        result.setdefault(k,[]).append(v)
    return result


def project(R, p):
    return [p(t) for t in R]



def distance(origin, destination):
    lat1 = origin[0] 
    lon1 = origin[1]
    lat2 = destination[0] 
    lon2 = destination[1]
    radius = 3959 #miles

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d


def counter(ls):
    result =[]
    for k,v in ls.items():
        take = []
        for i in v:
            if i[1] <= 1.5:
                take.append(i[0])
            else:
                take == take
        result.append(((k),take))
    return dict(result)








class mergeHealthRestaurant(dml.Algorithm):
    contributor = 'eileenli_yidingou'
    reads = ['eileenli_yidingou.health', 'eileenli_yidingou.Restaurants']
    writes = ['eileenli_yidingou.mergeHealthRestaurant_data']


    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_yidingou', 'eileenli_yidingou')

        # loads the collection
        HP = repo['eileenli_yidingou.health'].find()
        RS = repo['eileenli_yidingou.Restaurants'].find()


        hp_cord = {}
        for k in HP:
            for i in k:
                if i == 'name' or i == 'location':
                    #print(k['location']['coordinates'])
                    try:
                        hp_cord.setdefault(k['name'],k['location']['coordinates']).append()
                    except:
                        pass

        rs_cord = {}
        for k in RS:
            for i in k:
                if i == 'businessname' or i == 'location':
                    try:
                        rs_cord.setdefault(k['businessname'],k['location']['coordinates']).append()
                    except:
                        pass
        
        all_cord = cro(hp_cord,rs_cord)
        all_cord = comb(all_cord)
        re_cord = counter(all_cord)


        repo.dropCollection("Ent_Restaurants")
        repo.createCollection("Ent_Restaurants")

        setdis=[]

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_yidingou', 'eileenli_yidingou')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<fileEnRestaurants> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#mergeHealthRestaurant',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_health = doc.entity('dat:eileenli_yidingou#health',
                                             {'prov:label': 'health',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_restaurants = doc.entity('dat:eileenli_yidingou#restaurants',
                                             {'prov:label': 'restaurants',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        

        get_HealthRestaurants = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_HealthRestaurants, this_script)
        doc.usage(get_HealthRestaurants, resource_health, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_HealthRestaurants, resource_restaurants, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        

        HealthRestaurants = doc.entity('dat:eileenli_yidingou#health_Restaurants',
                          {prov.model.PROV_LABEL: 'health restaurants',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(HealthRestaurants, this_script)
        doc.wasGeneratedBy(HealthRestaurants, get_HealthRestaurants, endTime)
        doc.wasDerivedFrom(HealthRestaurants, resource_health, get_HealthRestaurants, get_HealthRestaurants, get_HealthRestaurants)
        doc.wasDerivedFrom(HealthRestaurants, resource_restaurants, get_HealthRestaurants, get_HealthRestaurants, get_HealthRestaurants)
        
        repo.logout()

        return doc

mergeHealthRestaurant.execute()
doc = mergeHealthRestaurant.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof