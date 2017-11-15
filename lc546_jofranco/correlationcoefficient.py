import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
import math
import decimal

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

        ''' sample inputs to the Double for loop
            - part of the idea that we want to put a restaurant in the ideal location without sacrificing the safety of customents
            -resturants near hubway: [{'numberRestaurantsnear': 9, 'location': [-71.10061883926392, 42.34011512249236]}
            -restaruants near a crime scene: [{'numberRestaurantsnear': 20, 'location': ['-71.10379454', '42.34638135']},
            -sample output shouldlook like so: (9,20)
        '''
        if trial == True:
            print("Warning, you are finding the statistical analysis in trial mode - a shortened version of the data.")
            hello = []
            restaurantcrimelist = []
            restauranthubwaylist = []
            for i in hubs.find()[0:100]:
                count = 0
                for j in crimes.find()[0:100]:
                    if (float(j['location'][0]) < i['location'][0] + .01) and (float(j['location'] [0]) > i['location'][0] - .01):
                        hello.append((i['numberRestaurantsnear'], j['numberRestaurantsnear']))
                        count = count+1
        else:
            hello = []
            restaurantcrimelist = []
            restauranthubwaylist = []
            for i in hubs.find()[0:200]:
                count = 0
                for j in crimes.find()[0:200]:
                    if (float(j['location'][0]) < i['location'][0] + .01) and (float(j['location'] [0]) > i['location'][0] - .01):
                        hello.append((i['numberRestaurantsnear'], j['numberRestaurantsnear']))
                    count = count+1

        restaurantcrimelist = [yi for (xi, yi) in hello]
        restauranthubwaylist = [xi for (xi, yi) in hello]
        print("Statistic Analysis printed below:")
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
        print("now doing correlation...")
        correlationfoodnearhubwayandcrime = correlationcoefficient.corr(restauranthubwaylist, restaurantcrimelist)
        print("The correlation is: ", correlationfoodnearhubwayandcrime)
        print("now doing pvalue. this may take a while...")
        pvalue = correlationcoefficient.p(restauranthubwaylist, restaurantcrimelist, trial)
        print("the p-value is: ")
        print(decimal.Decimal(pvalue))

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
        doc.add_namespace('bob', 'https://data.cityofboston.gov/resource/fdxy-gydq.json/')
        this_script = doc.agent('alg:lc546_jofranco#correlationcoefficient', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        CrimeRestaurantsinfo =  doc.entity('bob:fdxy-gydq', {'prov:label': 'Restaurants near a crime scene', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        HubwayRestaurantsinfo =  doc.entity('bob:fdxy-gydq', {'prov:label': 'Restaurants near a hubway stop', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource = doc.entity('bdp:xgbq-327x', {'prov:label':'Hubway nearby', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_correlationcoeffinfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Find the Correlation coefficient of restaurants near hubways and crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(get_correlationcoeffinfo, this_script)
        doc.usage(get_correlationcoeffinfo, resource, startTime)
        doc.wasAttributedTo(CrimeRestaurantsinfo,this_script)
        doc.wasAttributedTo(HubwayRestaurantsinfo,this_script)
        doc.wasGeneratedBy(CrimeRestaurantsinfo,  get_correlationcoeffinfo, endTime)
        doc.wasGeneratedBy(HubwayRestaurantsinfo,  get_correlationcoeffinfo, endTime)
        doc.wasDerivedFrom(CrimeRestaurantsinfo,  resource, get_correlationcoeffinfo, get_correlationcoeffinfo, get_correlationcoeffinfo)
        doc.wasDerivedFrom(HubwayRestaurantsinfo,  resource, get_correlationcoeffinfo, get_correlationcoeffinfo, get_correlationcoeffinfo)
        return doc

correlationcoefficient.execute()
doc = correlationcoefficient.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
