import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
import sys
import math
import numpy as np

class find_k_means(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds']
    writes = [] # Our solution is not really a collection

    @staticmethod
    def dist(p1, p2):
        return math.sqrt( (p2[0]-p1[0])**2 + (p2[1]-p1[1])**2 )

    @staticmethod
    def execute(trial=False):
        '''Select the addresses of important buildings from the Property Assessment data set'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo
        collection = db['bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds']


        # This is a constant that we can change if we want to
        K = 6

        # Set up the lat and long coordinates
        building_list = []
        for building in collection.find():
            if building['LATITUDE'] == '#N/A' or building['LONGITUDE'] == '#N/A':
                pass
            else:
                building_list.append( (float(building['LATITUDE']), float(building['LONGITUDE']) ) )

        kmeans = KMeans(n_clusters=K)
        kmeans.fit(building_list)

        # Build the dictionary and put it in a list because that's how we insert things in this part of town
        kmean_dic = {}
        kmean_dic['KMEAN'] = kmeans.cluster_centers_.tolist()
        kmean_list = [kmean_dic]

        # This is kind of repetitive we can probably try and find a better solution
        for p1 in kmean_list[0]['KMEAN']:
            min_dist = sys.maxsize
    
            # Iterate through important buildings and find closest one (aka min_dist)
            for building in collection.find():
                if building['LATITUDE'] == '#N/A' or building['LONGITUDE'] == '#N/A':
                    pass
                else:
                    p2 = ( float(building['LATITUDE']), float(building['LONGITUDE']) )
                    distance = math.sqrt( (p2[0]-p1[0])**2 + (p2[1]-p1[1])**2 )
                    if distance < min_dist:
                        min_dist = distance
                        last_building = building
            print(min_dist, last_building)
            print()

        repo.dropCollection('bkin18_cjoe_klovett_sbrz.kmeans')
        repo.createCollection('bkin18_cjoe_klovett_sbrz.kmeans')
        repo['bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds'].insert_many(kmean_list)
  
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#DataSet')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#find_k_means',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        #This line might need some modification - Keith
        resource = doc.entity('bdp:38173410110330', {'prov:label': 'K-Means', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        property_address_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#property_assessment_impBuilds',
            {'prov:label': 'property_assessment_impBuilds', prov.model.PROV_TYPE: 'ont:DataSet'})
        kmean_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#kmeans',
                                {'prov:label': 'K-Means Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        select_impBuilds_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(select_impBuilds_data, this_script)
        #I don't think this is actually of type "retrieval," I'm just not sure what the actual name for it is atm. - Keith
        doc.usage(select_impBuilds_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=address+data&$select=description'
                  }
                  )

        doc.wasAttributedTo(kmean_db, this_script)
        doc.wasGeneratedBy(kmean_db, select_impBuilds_data, endTime)
        doc.wasDerivedFrom(kmean_db, resource, kmean_db)

        repo.logout()

        return doc
