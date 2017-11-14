import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

#calculations to get correlation for # crimes per street : # of properties per street
class calculateCorrelations(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.property_crimes']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations']


    # Taking all the helper functions given in class by Professor Lapets
    
    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def avg(x): # Average
        return sum(x)/len(x)
    
    def stddev(x): # Standard deviation.
        m = avg(x)
        return sqrt(sum([(xi-m)**2 for xi in x])/len(x))
    
    def cov(x, y): # Covariance.
        return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)
    
    def corr(x, y): # Correlation coefficient.
        if stddev(x)*stddev(y) != 0:
            return cov(x, y)/(stddev(x)*stddev(y))
        
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

#         url = 'http://datamechanics.io/data/eileenli_yidingou/Restaurant.json'
#         response = urllib.request.urlopen(url).read().decode("utf-8")
#         r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)

        # code to calculate the correlation
        # grabbing all the x and y from first read file
        crimes_properties = repo[reads[0]]
        x_vals = []
        y_vals = []

        for key in crimes_properties.find():
            x_vals.append(crimes_properties[key]['Crimes'])
            y_vals.append(crimes_properties[key]['Properties'].count())
            
        correlation1 = corr(x_vals, y_vals)
        
        finalData = dict()
        finalData['crimes_propertyLoc'] = correlation1
        #finalData =  {'crimes': 5, 'properties' : 6, 'xyz' : 7 }        
            
        repo.dropCollection("calculateCorrelations")
        repo.createCollection("calculateCorrelations")
        
        repo['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations'].insert_many(finalData)
        repo['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations'].metadata())
        

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

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#calculateCorrelations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Restaurants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Restaurants, this_script)
        doc.usage(get_Restaurants, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=BostonLife+Restaurants&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        Restaurants = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#Restaurants', {prov.model.PROV_LABEL:'BostonLife Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Restaurants, this_script)
        doc.wasGeneratedBy(Restaurants, getRestaurants, endTime)
        doc.wasDerivedFrom(Restaurants, resource, getRestaurants, getRestaurants, getRestaurants)

        repo.logout()
                  
        return doc

# example.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

