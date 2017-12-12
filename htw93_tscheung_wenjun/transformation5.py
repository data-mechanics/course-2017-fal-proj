import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty
import scipy.stats
from sklearn.cluster import KMeans
from sklearn import preprocessing
import numpy as np
import matplotlib.pyplot as plt
import itertools

def Clusts(clustnum,labels_array):
    return (np.where(labels_array == clustnum)[0])

class transformation5(dml.Algorithm):
# Set up the database connection.
    contributor = 'htw93_tscheung_wenjun'
    reads = ['htw93_tscheung_wenjun.BostonHotelCustomScore','htw93_tscheung_wenjun.BostonHotel']
    writes = ['htw93_tscheung_wenjun.BostonHotelPotentialPermutation']

    def get_one_permutation(origin, garden, food, mbta,trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')
        BostonHotelCustomScore = repo.htw93_tscheung_wenjun.BostonHotelCustomScore
        BostonHotel = repo.htw93_tscheung_wenjun.BostonHotel
        #BostonHotelRatingOriginal = repo.htw93_tscheung_wenjun.BostonHotel
        #HotelRaing = BostonHotelRatingOriginal.find()
        hotel_score = BostonHotelCustomScore.find()
        hotel = BostonHotel.find()
        
        coordinate = []
        hotel_lists = []
        for s in hotel_score:
            hotel_lists.append(s['hotel'])
            for h in hotel:
                if s['hotel'] == h['Hotel_name']:
                    coordinate.append([h['lat'],h['lon']])
                    break
            hotel.rewind()
        hotel_score.rewind()
        
        coordinates = preprocessing.scale(coordinate) * 3
        
        norm_rate = []
        norm_mbta =[]
        norm_garden =[]
        norm_food =[]
        orginal_rate=[]
        # Combine boston crime and boston schools.
        res = []

        combine_rate_crime=[]
        count = 0
        
        for h in hotel_score:
            #print(h)
            orginal_rate.append([h['orginal_rate']])
            norm_rate.append([h['norm_rate']])
            norm_mbta.append([h['norm_mbta']])
            norm_garden.append([h['norm_garden']])
            norm_food.append([h['norm_food']])

            combine_rate_crime.append((h['norm_rate']+h['norm_mbta']+h['norm_garden']+h['norm_food'])/4)
            res.append([coordinates[count][0],coordinates[count][1],(h['norm_rate']*(origin+1) +h['norm_mbta']*(mbta+1) +h['norm_garden']*(garden+1) +h['norm_food']*(food+1))])
            count+=1
        hotel_score.rewind()
        

        if trial:
            combine_rate_crime=combine_rate_crime[:100]
            res= res[:100]
        

        X = np.array(res)
        kmeans = KMeans(n_clusters=10, random_state=0).fit(X)
        kmeans_arr_mbta = kmeans.cluster_centers_

        ids = []
        scores = []
        for i in range(10):
            cluster_list = Clusts(i,kmeans.labels_).tolist()
            ids.append(cluster_list)
            temp = 0
            for j in range(len(cluster_list)):
                temp += combine_rate_crime[cluster_list[j]]
            scores.append(temp/len(cluster_list))
        #scores = [[combine_rate_crime[i] for i in x] for x in ids]

        
        idx = [len(i) for i in ids]
        max_index_mbta = idx.index(max(idx))
        #print (kmeans_arr_mbta)
        #print(idx)
        #print(kmeans_arr_mbta[max_index_mbta])
        #print(scores)

        
        #fig = plt.figure(figsize=(16,12))
        #colors = ['red','blue','green','pink','yellow','cyan','black','orange','lightblue','lightgreen']
        cluster=[]
        max_ids = ids[scores.index(max(scores))]
        for i in range(len(max_ids)):
            cluster.append({'hotel':hotel_lists[max_ids[i]],'lat':coordinate[max_ids[i]][0],'long':coordinate[max_ids[i]][1],'score':res[max_ids[i]][2]})

        # for i in range(len(kmeans.cluster_centers_)):
        #     label = scores[i]
        #     x = kmeans.cluster_centers_[i][0]
        #     y = kmeans.cluster_centers_[i][1]
        #     #plt.annotate(label, xy=(x,y), xytext=(x, y))
        # for i in range(10):
        #     indexs = Clusts(i,kmeans.labels_)
        #     #x = []
        #     #y = []
        #     #print(indexs)
        #     tmp = []
        #     for entry in indexs:
        #         x = []
        #         x.append([hotel_lists[entry]])
        #         #print(x)
        #         y = coordinate[entry]
        #         x.append(y)
        #         tmp.append(x)
        #     cluster.append(tmp)
        
        coorx_sum = 0
        coory_sum = 0
        for idx in ids[5]:
            coorx_sum+= coordinate[idx][0]
            coory_sum+= coordinate[idx][1]
        print("*********")
        print(coorx_sum/len(ids[5]),coory_sum/len(ids[5]))
        print("***********")
        
        key  = str(origin)+str(garden)+str(food)+str(mbta)

        repo['htw93_tscheung_wenjun.BostonHotelPotentialPermutation'].insert({'id':key,'lat':coorx_sum/len(ids[5]),'long':coory_sum/len(ids[5]),'cluster': cluster})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    @staticmethod
    def execute(trial = False):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')
        repo.dropCollection("BostonHotelPotentialPermutation")
        repo.createCollection("BostonHotelPotentialPermutation")

        permutation_list = list(itertools.permutations([0, 1, 2, 3]))

        for entry in permutation_list:
            origin = entry[0]
            garden = entry[1]
            food = entry[2]
            mbta = entry[3]
            print(origin,garden,food,mbta)
            transformation5.get_one_permutation(origin,garden,food, mbta)
            #doc = transformation4_parameters.provenance()

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # define entity to represent resources
        Permutation_script = doc.agent('alg:htw93_tscheung_wenjun#transformation5', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_BostonHotelCustomScore = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelCustomScore', {'prov:label': 'BostonHotelCustomScore', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_BostonHotel = doc.entity('dat:htw93_tscheung_wenjun#BostonHotel', {'prov:label': 'BostonHotel', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        
        # define activity to represent invocation of the script
        get_BostonHotelPermutation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

    
        # associate the activity with the script
        doc.wasAssociatedWith(get_BostonHotelPermutation, Permutation_script)
        
        # indicate that an activity used the entity
        doc.usage(get_BostonHotelPermutation, resource_BostonHotelCustomScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_BostonHotelPermutation, resource_BostonHotel, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        obtained_BostonHotelPotentialPermutation = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelPotentialPermutation', {prov.model.PROV_LABEL: 'BostonHotelPotentialPermutation', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(obtained_BostonHotelPotentialPermutation, Permutation_script)
        doc.wasGeneratedBy(obtained_BostonHotelPotentialPermutation, get_BostonHotelPermutation, endTime)
        doc.wasDerivedFrom(obtained_BostonHotelPotentialPermutation, resource_BostonHotelCustomScore, get_BostonHotelPermutation, get_BostonHotelPermutation, get_BostonHotelPermutation)
        doc.wasDerivedFrom(obtained_BostonHotelPotentialPermutation, resource_BostonHotel, get_BostonHotelPermutation, get_BostonHotelPermutation, get_BostonHotelPermutation)

        repo.logout()
                  
        return doc
            

#transformation5.execute()
#doc = transformation5.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

