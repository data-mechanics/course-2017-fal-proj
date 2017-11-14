import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
import math

class correlationcoefficient(dml.Algorithm):
    contributor = "lc546_jofranco"
    reads = ['lc546_jofranco.HubwayRestaurants', 'lc546_jofranco.CrimeRestaurants']
    # we are just outputing a number so no need for new file
    writes = []
    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def avg(x):
        return sum(x)/len(x)

    def stddev(x):
        m = correlationcoefficient.avg(x)
        return math.sqrt(sum([(xi - m)**2 for xi in x])/len(x))

    def cov(x, y):
        return sum([(xi-correlationcoefficient.avg(x))*(yi-correlationcoefficient.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

    def corr(x, y):
        if correlationcoefficient.stddev(x)*correlationcoefficient.stddev(y) != 0:
            return correlationcoefficient.cov(x, y)/(correlationcoefficient.stddev(x)*correlationcoefficient.stddev(y))

    def p(x,y, trial):
        c0 = correlationcoefficient.corr(x,y)
        corrs = []
        if trial == True:
            for k in range(0,200):
                y_permuted = correlationcoefficient.permute(y)
                corrs.append(correlationcoefficient.corr(x,y_permuted))
            return len([c for c in corrs if abs(c) > c0])/len(corrs)
        for k in range(0,2000):
            y_permuted = correlationcoefficient.permute(y)
            corrs.append(correlationcoefficient.corr(x,y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)

    @staticmethod
    def execute(trial=True):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco','lc546_jofranco')
        crimes = repo.lc546_jofranco.CrimeRestaurants
        hubs = repo.lc546_jofranco.HubwayRestaurants
        # sample inputs to the Double for loop
        # part of the idea that we want to put a restaurant in the ideal location without sacrificing the safety of customents
        # resturants near hubway: [{'numberRestaurantsnear': 9, 'location': [-71.10061883926392, 42.34011512249236]}
        # restaruants near a crime scene: [{'numberRestaurantsnear': 20, 'location': ['-71.10379454', '42.34638135']},
        #(9,20)
        #r tree index
        # use a transformation?
        if trial == True:
            hello = []
            restaurantcrimelist = []
            restauranthubwaylist = []
            for i in hubs.find()[0:80]:
                #print(i)
                count = 0
                for j in crimes.find()[0:80]:
                    #print(j)
                    #print(i['location'][0])
                    if (float(j['location'][0]) < i['location'][0] + .01) and (float(j['location'] [0]) > i['location'][0] - .01):
                        hello.append((i['numberRestaurantsnear'], j['numberRestaurantsnear']))
                        #restauranthubwaylist.append(i['numberRestaurantsnear'])
                        #restaurantcrimelist.append(j['numberRestaurantsnear'])
                        count = count+1
        else:
            hello = []
            restaurantcrimelist = []
            restauranthubwaylist = []
            for i in hubs.find():
                #print(i)
                count = 0
                for j in crimes.find():
                    #print(j)
                    #print(i['location'][0])
                    if (float(j['location'][0]) < i['location'][0] + .01) and (float(j['location'] [0]) > i['location'][0] - .01):
                        hello.append((i['numberRestaurantsnear'], j['numberRestaurantsnear']))
                    #restauranthubwaylist.append(i['numberRestaurantsnear'])
                    #restaurantcrimelist.append(j['numberRestaurantsnear'])
                    count = count+1

        restaurantcrimelist = [yi for (xi, yi) in hello]
        restauranthubwaylist = [xi for (xi, yi) in hello]
        #print(restaurantcrimelist)
        #print(hello)
        print("Statistic Analysis printed below :D")
        print("working on averages")
        averagefoodnearhubway = correlationcoefficient.avg(restauranthubwaylist)
        averagefoodnearcrime = correlationcoefficient.avg(restaurantcrimelist)
        print("finished averages, now doing standard deviation")
        stdevfoodnearhubway = correlationcoefficient.stddev(restauranthubwaylist)
        stdevfoodnearcrime = correlationcoefficient.stddev(restaurantcrimelist)
        print("Standard deviation for hubway and restaurants near station: ", stdevfoodnearhubway)
        print("Standard deviation for crime and restaurants near that crime scene: ", stdevfoodnearcrime)
        print("finished foing standard deviation, now doing covariance.")
        covariancefoodnearhubwayandcrime = correlationcoefficient.cov(restauranthubwaylist, restaurantcrimelist)
        print("the covariance is: ", covariancefoodnearhubwayandcrime)
        #covariancefoodnearcrime = correlationcoefficient.cov(restaurantcrimelist)
        print("now doing correlation...")
        correlationfoodnearhubwayandcrime = correlationcoefficient.corr(restauranthubwaylist, restaurantcrimelist)
        print("The correlation is: ", correlationfoodnearhubwayandcrime)
        #correlationfoodnearcrime = correlationcoefficient.corr(restaurantcrimelist)
        print("now doing pvalue. this may take a while...")
        pvalue = correlationcoefficient.p(restauranthubwaylist, restaurantcrimelist, trial)
        print("the p-value is: ", pvalue)

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://secure.thehubway.com/data/')
        this_script = doc.agent('alg:lc546_jofranco#hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'Hubway nearby', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bikeinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'hubway', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_bikeinfo, this_script)
        doc.usage(get_bikeinfo, resource, startTime)
        Bikeinfo = doc.entity('dat:lc546_jofranco#Bikeinfo', {prov.model.PROV_LABEL:'Hubway Bike info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Bikeinfo, this_script)
        doc.wasGeneratedBy(Bikeinfo, get_bikeinfo, endTime)
        doc.wasDerivedFrom(Bikeinfo, resource, get_bikeinfo, get_bikeinfo, get_bikeinfo)
        return doc

correlationcoefficient.execute()
doc = correlationcoefficient.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
