import dml
import urllib.request
import prov.model
import uuid
import datetime
import json
import requests
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap
#import image

def evaluate_clusters(X,max_clusters):
    error = np.zeros(max_clusters+1)
    error[0] = 0;
    for k in range(1,max_clusters+1):
        kmeans = KMeans(init='k-means++', n_clusters=k, n_init=10)
        kmeans.fit_predict(X)
        error[k] = kmeans.inertia_

    plt.plot(range(1,len(error)),error[1:])
    plt.xlabel('Number of clusters')
    plt.ylabel('Error')
    plt.show()


class kmeans(dml.Algorithm):
    contributor = "lc546_jofranco"
    reads = ["lc546_jofranco.propety"]
    writes = ["lc546_jofranco.kmeans"]

    @staticmethod
    def execute(trial = False):
        responsetext = open("kmeans.txt", "w")
        kmeansclusters = open("clusters.txt", "w")
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        startTime = datetime.datetime.now()
        #url = 'https://data.boston.gov/export/cec/df0/cecdf003-9348-4ddb-94e1-673b63940bb8.json'
        #url = 'https://data.boston.gov/export/cec/df0/cecdf003-9348-4ddb-94e1-673b63940bb8.json'
        #response = open('/Users/Jesus/Desktop/project1/course-2017-fal-proj/lc546_jofranco/propetyplain.txt').read()
        #print(response)
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        #r = json.loads(response)
        r = requests.get('https://data.boston.gov/export/cec/df0/cecdf003-9348-4ddb-94e1-673b63940bb8.json')
        t = r.text.replace("\n],\n", ",\n")
        p = json.loads('{"data":'+t+']}')
        #(p['data']))
        latitude = []
        longitude = []
        lalo = []
        lalo1 = []
        zipcode = []
        for i in p['data']:
            zipcode.append(i['MAIL_ZIPCODE'].replace("_", ""))
            if ((i['LATITUDE']!='#N/A' and i['LATITUDE']!="" ) and (i['LONGITUDE']!='#N/A' and i['LONGITUDE']!="" )):
                latitude.append(i['LATITUDE'])
                longitude.append(i['LONGITUDE'])
        for i in range(118):
            lalo +=[latitude[i], longitude[i]]
            lalo1.append("[ " + str(latitude[i]) + "," + str(longitude[i]) + "]")  
            responsetext.write(str(lalo))
            responsetext.write(str(" "))
        total = {'zipcode': zipcode, 'address': lalo}
        location = total

        #s = json.dumps(t, sort_keys= True, indent = 2)
        #pr{t(r)
        #r = response


        # zipcode = []
        # lalo = []
        # street = []
        # latitude = []
        # longitude = []

        # for i in r:
        #     print("####################################")
        #     print(r.find("LATITUDE"))
        #     # if ((i['latitude']!='#N/A' and i['latitude']!="" ) and (i['longitude']!='#N/A' and i['longitude']!="" )):
        #     #     latitude.append(i['latitude'])
        #     #     longitude.append(i['longitude'])
        #     #     zipcode.append( i['zipcode'])

        # for i in range(118):
        #     lalo += [latitude[i], longitude[i]]
        # print(len(latitude), len(longitude))
        # total = {'zipcode': zipcode, 'address': lalo, 'street': street}



        location = total['address']
        x = latitude
        y = longitude




        # #evaluate_clusters(location, 8)
        kmeans = KMeans(n_clusters = 1).fit(location)
        kmeansclusters.write(str(kmeans.cluster_centers_))
        kmeansclusters.close()
        #print(kmeans.cluster_centers_)

        centers = [x[:2] for x in kmeans.cluster_centers_]
        print(centers)
    #    try:
        #    map = Basemap(projection='merc', lat_0 = 42, lon_0 = -71,
        #        resolution = 'h', area_thresh = 0.99,
        #        llcrnrlon=-73.25, llcrnrlat=45.0,
        #        urcrnrlon=-69.25, urcrnrlat=39.75)
        #    map.drawcoastlines()
        #    map.drawcountries()
        #    map.fillcontinents(color = 'coral')
        #    map.drawmapboundary()
        #    map.plot(x, y, 'bo', markersize=18)
            #t = np.random.rand(20)
        #    plt.scatter(x,y)
        #    plt.xlabel('Latitude')
        #    plt.ylabel('Longitude')
        #    plt.savefig("kmeans.png")
        #    print("plot has been created. please look for it within the directory")
        #    print(centers)
        try:
            t = np.random.rand(20)
            plt.scatter(x,y, c=t)
            plt.xlabel('Latitude')
            plt.ylabel('Longitude')


            plt.savefig("kmeans.png")
            print("plot has been created. please look for it within the directory")
        #    plt.show()
    #    except ValueError:
    #        print("You have exited out of the plot. Continuing on! :D")


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        # Provenance Data
        doc = prov.model.ProvDocument()
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:lc546_jofranco#kmeans', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        constraint = doc.entity('dat:lc546_jofranco#propety', {prov.model.PROV_LABEL:'Returns whether or not the constraint is satisfied', prov.model.PROV_TYPE:'ont:DataSet'})


        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, constraint, startTime)

        # Our new combined data set
        maintenance = doc.entity('dat:lc546_jofranco#kmeans', {prov.model.PROV_LABEL:'finds centers using kmeans', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(maintenance, this_script)
        doc.wasGeneratedBy(maintenance, this_run, endTime)
        doc.wasDerivedFrom(maintenance, constraint, this_run, this_run, this_run)

        #repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


# running in trial mode won't show you good clusters since we aren't looking at the full data set
kmeans.execute()
doc = kmeans.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
