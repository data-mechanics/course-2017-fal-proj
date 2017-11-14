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



def Clusts(clustnum,labels_array):
    return (np.where(labels_array == clustnum)[0])

class transformation5(dml.Algorithm):
    contributor = 'htw93_tscheung_wenjun'
    reads = ['htw93_tscheung_wenjun.BostonHotelCustomScore','htw93_tscheung_wenjun.BostonHotel']
    writes = ['htw93_tscheung_wenjun.BostonHotelPotential']
    
    @staticmethod
    def execute(trial = False):
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
            print(h)
            orginal_rate.append([h['orginal_rate']])
            norm_rate.append([h['norm_rate']])
            norm_mbta.append([h['norm_mbta']])
            norm_garden.append([h['norm_garden']])
            norm_food.append([h['norm_food']])

            combine_rate_crime.append((h['norm_rate']+h['norm_mbta']+h['norm_garden']+h['norm_food'])/4)
            res.append([coordinates[count][0],coordinates[count][1],(h['norm_rate']+h['norm_mbta']+h['norm_garden']+h['norm_food'])/4])
            count+=1
        hotel_score.rewind()
        
        
        

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
        print(idx)
        print(kmeans_arr_mbta[max_index_mbta])
        print(scores)
        
        fig = plt.figure(figsize=(16,12))
        colors = ['red','blue','green','pink','yellow','cyan','black','orange','lightblue','lightgreen']
        for i in range(len(kmeans.cluster_centers_)):
            label = scores[i]
            x = kmeans.cluster_centers_[i][0]
            y = kmeans.cluster_centers_[i][1]
            plt.annotate(label, xy=(x,y), xytext=(x, y))
        for i in range(10):
            indexs = Clusts(i,kmeans.labels_)
            x = []
            y = []
            for j in range(len(indexs)):
                if i == 5:
                    print(hotel_lists[indexs[j]])
                print(res[indexs[j]][0],res[indexs[j]][1])
                x.append(res[indexs[j]][0])
                y.append(res[indexs[j]][1])
            plt.scatter(x, y, color=colors[i],s = 50,label=scores[i])
        plt.legend(loc = 'upper left', fontsize = 'medium')
        plt.title("K-means Plot")

        fig.savefig('temp.png', dpi=fig.dpi)
        
        coorx_sum = 0
        coory_sum = 0
        for idx in ids[5]:
            coorx_sum+= coordinate[idx][0]
            coory_sum+= coordinate[idx][1]
        print("*********")
        print(coorx_sum/len(ids[5]),coory_sum/len(ids[5]))
        print("***********")
        

        

        print('Finished creating collection htw93_tscheung_wenjun.BostonHotelCorrelation')
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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
        Potential_script = doc.agent('alg:htw93_tscheung_wenjun#transformation5', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_BostonHotelCustomScore = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelCustomScore', {'prov:label': 'BostonHotelCustomScore', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_BostonHotel = doc.entity('dat:htw93_tscheung_wenjun#BostonHotel', {'prov:label': 'BostonHotel', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        
        # define activity to represent invocation of the script
        get_BostonHotelPotential = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

    
        # associate the activity with the script
        doc.wasAssociatedWith(get_BostonHotelPotential, Potential_script)
        
        # indicate that an activity used the entity
        doc.usage(get_BostonHotelPotential, resource_BostonHotelCustomScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_BostonHotelPotential, resource_BostonHotel, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        obtained_BostonHotelPotential = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelPotential', {prov.model.PROV_LABEL: 'BostonHotelPotential', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(obtained_BostonHotelPotential, Potential_script)
        doc.wasGeneratedBy(obtained_BostonHotelPotential, get_BostonHotelPotential, endTime)
        doc.wasDerivedFrom(obtained_BostonHotelPotential, resource_BostonHotelCustomScore, get_BostonHotelPotential, get_BostonHotelPotential, get_BostonHotelPotential)
        doc.wasDerivedFrom(obtained_BostonHotelPotential, resource_BostonHotel, get_BostonHotelPotential, get_BostonHotelPotential, get_BostonHotelPotential)

        repo.logout()
                  
        return doc



transformation5.execute()
doc = transformation5.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
