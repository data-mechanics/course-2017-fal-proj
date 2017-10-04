import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty
from collections import defaultdict

class threeBigBellies(dml.Algorithm):
    contributor = 'adsouza_mcsmocha'
    reads = ['adsouza_mcsmocha.ThreeReq', 'adsouza_mcsmocha.BigBelly']
    writes = ['adsouza_mcsmocha.ThreeBigBellies']

    @staticmethod
    def execute(trial=False):
        # Retrieve some data sets (not using the API here for the sake of simplicity).
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')
        repo.dropCollection('BigBellyandThreeTypes')
        repo.createCollection('BigBellyandThreeTypes')

        bigBelly = repo['adsouza_mcsmocha.BigBelly'].find()
        threeReq = repo['adsouza_mcsmocha.ThreeReq'].find()

        # Select entries in 311Requests that pertain to Trash violations using list comprehension
        select311Trash = [e for e in threeReq if 'Trash' in e['TYPE']]

        # Selectively take out entries we don't need for comparison
        def select311TrashDel(dictList):
            newDictList = []
            for each_dict in dictList:
                new_dict = {}
                new_dict['neighborhood'] = each_dict['neighborhood']
                new_dict['Latitude'] = each_dict['Latitude']
                new_dict['Longitude'] = each_dict['Longitude']
                newDictList.append(new_dict)

            return newDictList

        select311TrashReduced = select311TrashDel(select311Trash)

        # Get latitudes and longitudes for 311Requests that pertain to trash and rounds them up for comparison
        select311Lats = []
        select311Longs = []

        for i, lats in enumerate(d['Latitude'] for d in select311TrashReduced):
            select311Lats.append(lats)

        for i, longs in enumerate(d['Longitude'] for d in select311TrashReduced):
            select311Longs.append(longs)

        # Projecting lats and longs into tuples into the 311 dataset
        def zip311coords(dictList):
            ''' Put lats, longs into the tuples as coordinates because Big Belly uses the same tuple format 
                and we want to compare them using geopy later
            '''
            coordinates = list(zip(select311Lats, select311Longs))
            i = 0

            # Appends them to corresponding dict
            newDictList = []
            for each_dict in dictList:
                new_dict = {}
                new_dict['neighborhood'] = each_dict['neighborhood']
                new_dict['Coordinates'] = coordinates[i]
                i += 1
            return newDictList

        select311TrashNew = zip311coords(select311TrashReduced)

        def selectBigBellyDel(dictList):
            newDictList = []
            for each_dict in dictList:
                new_dict = {}
                new_dict['fullness'] = each_dict['fullness']
                new_dict['collection'] = each_dict['collection']
                new_dict['Location'] = each_dict['Location']
                newDictList.append(new_dict)
            return newDictList

        selectBigBellyReduced = selectBigBellyDel(bigBelly)

        def aggregateScore(dictList1, dictList2):
            neighborhood_scores_dataset = []
            neighborhoods_list = []

            # Get all neighborhoods into a list
            for each_dict in dictList1:
                if each_dict['neighborhood'] not in neighborhoods_list:
                    neighborhoods_list.append(each_dict['neighborhood'])

            # Create template for neighborhood_scores
            for item in neighborhoods_list:
                new_dict = {}
                new_dict[item] = 0
                neighborhood_scores_dataset.append(new_dict)

                # Aggregates neighborhoods with 311's coordinates and BigBelly's locations
                for each_dict2 in dictList2:
                    for each_dict1 in dictList1:
                        distance = vincenty(each_dict1['Coordinates'], each_dict2['Location']).miles
                        if distance <= 0.5:
                            neighborhood_scores_dataset.append({each_dict1['neighborhood'], 1})

                            # for d in neighborhood_scores_dataset:
                            #   d.update((k, v+1) for k, v in d.items() if k == each_dict1['neighborhood'])

            return neighborhood_scores_dataset

        final_result = aggregateScore(select311TrashNew,selectBigBellyReduced)
        repo['adsouza_mcsmocha.ThreeBigBellies'].insert_many(final_result)
        repo.logout()
        endTime = datetime.datetime.now()
        return {'start': startTime, 'end': endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''Create the provenance document describing everything happening in this script. Each run of the script will generate a new document describing that invocation event.'''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_mcsmocha', 'adsouza_mcsmocha')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        # Additional resource
        doc.add_namespace('anb', 'https://data.boston.gov/')
        doc.add_namespace('bod','http://bostonopendata-boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:adsouza_mcsmocha#threeBigBellies',{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        big_belly_resource = doc.entity('anb:c8c54c49-3097-40fc-b3f2-c9508b8d393a',{'prov:label': 'Big Belly Alerts, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'json'})
        three_resource = doc.entity('anb:2968e2c0-d479-49ba-a884-4ef523ada3c0',{'prov:label': '311 Requests, Service Requests',prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'json'})
        get_bb = doc.activity('log:uuid' + str(uuid.uuid4()),startTime, endTime)
        get_three = doc.activity('log:uuid' + str(uuid.uuid4()),startTime, endTime)

        doc.wasAssociatedWith(get_bb, this_script)
        doc.wasAssociatedWith(get_three, this_script)
        doc.usage(get_bb, resource, startTime, None,{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Query': '?type=Big+Belly+Alerts&$description,timestamp,fullness,collection,location'})

        doc.usage(get_three, resource, startTime, None,{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Query': '?type=311+Requests&$CASE_TITLE,TYPE,QUEUE,Department,Location,pwd_district,neighborhood,neighborhood_services_district,LOCATION_STREET_NAME,LOCATION_ZIPCODE'})

        big_belly = doc.entity('dat:adsouza_mcsmocha#BigBelly',{prov.model.PROV_LABEL: 'Big Belly Alerts', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(big_belly, this_script)
        doc.wasGeneratedBy(big_belly, get_bb, endTime)
        doc.wasDerivedFrom(big_belly, resource, get_bb, get_bb, get_bb)

        req_311 = doc.entity('dat:adsouza_mcsmocha#ThreeReq',{prov.model.PROV_LABEL: '311 Requests',prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(req_311, this_script)
        doc.wasGeneratedBy(req_311, get_three, endTime)
        doc.wasDerivedFrom(req_311, resource, get_three, get_three,get_three)

        repo.logout()

        return doc


threeBigBellies.execute()
doc = threeBigBellies.provenance()
print (doc.get_provn())
print (json.dumps(json.loads(doc.serialize()), indent=4))
