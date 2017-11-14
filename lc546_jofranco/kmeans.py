import dml
import urllib.request
import prov.model
import uuid
import datetime
import json
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np

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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        startTime = datetime.datetime.now()
        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        print("#########################")

        zipcode = []
        lalo = []
        street = []
        latitude = []
        longitude = []

        for i in r:
            #if (len(zipcode)<120):


            #if (len(latitude)<120):
            if ((i['latitude']!='#N/A' and i['latitude']!="" ) and (i['longitude']!='#N/A' and i['longitude']!="" )):
                latitude.append(i['latitude'])

            #if (len(longitude)<120):
                #if (i['longitude']!='#N/A' and i['longitude']!="" ):
                longitude.append(i['longitude'])
                #print(longitude, i['longitude'])
                zipcode.append( i['zipcode'])

        print(len(latitude))
        print(len(longitude))

        for i in range(118):

            lalo += [latitude[i], longitude[i]]

            #street.append( i['st_name'])
        print("$$$$$$$$$$$$$$$$$$")
        print(len(latitude), len(longitude))
        #print(lalo)
        total = {'zipcode': zipcode, 'address': lalo, 'street': street}

        #print(total)
        #s = json.dumps(total, sort_keys= True, indent = 2)
        #    print(type(s))
        #repo.dropCollection("propety")
        #repo.createCollection("propety")
        #repo["lc546_jofranco.propety"].insert_many(total)
        #repo["lc546_jofranco.propety"].metadata({'complete':True})
        #print(repo["lc546_jofranco.propety"].metadata())
        #repo.dropPermanent("kmeans")
        #repo.createPermanent("kmeans")


        #values = repo.lc546_jofranco.propety.find()


        location = total['address']
        x = latitude
        y = longitude




        #evaluate_clusters(location, 8)
        kmeans = KMeans(n_clusters = 1).fit(location)
        centers = [x[:2] for x in kmeans.cluster_centers_]
        print(centers)
        plt.scatter(x,y)
        plt.xlabel('Latitude')
        plt.ylabel('Longitude')
        plt.show()

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

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


# running in trial mode won't show you good clusters since we aren't looking at the full data set
kmeans.execute()
doc = kmeans.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
